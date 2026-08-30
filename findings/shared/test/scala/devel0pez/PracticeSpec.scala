package devel0pez

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{broadcast, col}

/** The received wisdom about writing fast Spark, put to the plan on both engines.
  *
  * Every rule below is one people repeat, review for, and put in style guides. Each is checked the
  * only way that settles it: write the query the naive way and the recommended way, and compare the
  * **physical plans**. If the optimiser already does it, the two are the same plan and the advice
  * is folklore — real once, obsolete now, still being enforced in code review.
  *
  * Seven of the eight are exactly that, on both engines. Which is the useful result: the practices
  * worth arguing about are not these, they are the ones no optimiser can do for you — the shape of
  * the data, the types at the boundaries, whether a closure hides the predicate from the planner at
  * all (`PushdownSpec`).
  *
  * The eighth is the exception, and it is the interesting one: the only rewrite that still changes
  * a plan on classic is the one Sail does not implement.
  *
  * Two honest limits on all of this. The fixtures are tiny, so nothing here measures *time* — only
  * what the engine decided to do, which is the part that generalises. And a plan comparison is a
  * strong assertion: if a future Spark or Sail stops collapsing one of these, the test goes red,
  * which is the point.
  */
final class PracticeSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val fact = s"prac_fact_$suffix"
  private val dim = s"prac_dim_$suffix"

  private def dropAll(): Unit =
    Seq(fact, dim).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    // Parquet and five columns wide, so pruning and pushdown have somewhere to show.
    spark.sql(s"""CREATE TABLE $fact (
      country STRING, branch STRING, product STRING, amount DECIMAL(18,2), day TIMESTAMP
    ) USING parquet""")
    spark.sql(s"CREATE TABLE $dim (code STRING, name STRING, family STRING) USING parquet")
    spark.sql(s"""INSERT INTO $fact VALUES
      ('ES','0182','P1',100.50, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0227','P9', 33.33, TIMESTAMP'2026-01-19 00:00:00'),
      ('FR','0300','P1', 10.00, TIMESTAMP'2026-01-20 00:00:00')""")
    spark.sql(s"INSERT INTO $dim VALUES ('P1','Widget','TOOLS'), ('P9','Gadget','TOYS')")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def facts = spark.table(fact)
  private def dims = spark.table(dim)

  /** Assert the rewrite changed nothing the engine will act on, and nothing it returns. */
  private def sameWork(naive: DataFrame, rewritten: DataFrame): Unit = {
    withClue("the rewrite changed the physical plan: ") {
      Plans.shape(naive) shouldBe Plans.shape(rewritten)
    }
    naive.collect().map(_.toString).sorted.toSeq shouldBe
      rewritten.collect().map(_.toString).sorted.toSeq
  }

  "rules the optimiser already applies, on both engines" - {

    "chained withColumn versus one select" in {
      // The rule this project follows anyway — `select` says the shape in one
      // place — but the reason is legibility, not speed. `PlanSpec` counts the
      // projections; this compares the whole plan.
      sameWork(
        (1 to 4).foldLeft(facts)((df, i) => df.withColumn(s"c$i", col("amount") + i)),
        facts.select(
          facts.columns.map(col) ++ (1 to 4).map(i => (col("amount") + i).as(s"c$i")): _*
        )
      )
    }

    "filtering before the join versus after it" in {
      // The oldest one in the book. Both planners push the predicate through
      // the join to the scan on their own.
      sameWork(
        facts.join(dims, col("product") === col("code")).filter(col("country") === "ES"),
        facts.filter(col("country") === "ES").join(dims, col("product") === col("code"))
      )
    }

    "projecting before the join versus after it" in {
      // Same story for column pruning: writing the narrow `select` by hand
      // buys nothing, because the scan was already reading only what the
      // query needs.
      sameWork(
        facts.join(dims, col("product") === col("code")).select(col("branch"), col("family")),
        facts
          .select(col("branch"), col("product"))
          .join(dims.select(col("code"), col("family")), col("product") === col("code"))
          .select(col("branch"), col("family"))
      )
    }

    "distinct versus dropDuplicates" in {
      // Treated as different tools in a lot of code. Same plan, same aggregate.
      sameWork(
        facts.select(col("country")).distinct(),
        facts.select(col("country")).dropDuplicates()
      )
    }

    "filtering before the union versus after it" in {
      sameWork(
        facts.union(facts).filter(col("country") === "ES"),
        facts.filter(col("country") === "ES").union(facts.filter(col("country") === "ES"))
      )
    }
  }

  "orderBy followed by limit" - {

    "becomes a top-N rather than a full sort, on both" in {
      // The advice is "never sort just to take the first rows". Neither engine
      // does: classic emits `TakeOrderedAndProject`, DataFusion a `TopK` sort.
      // Different words, same decision.
      val plan = Plans.shape(facts.orderBy(col("amount").desc).limit(2))

      perEngine {
        plan should include("TakeOrderedAndProject")
      } {
        plan should include("TopK")
      }
    }
  }

  "the one rewrite that still does something" - {

    "is the broadcast hint, and only once the automatic one is out of the way" in {
      // With the default threshold the dimension is small enough that classic
      // broadcasts it regardless, so the hint looks useless. That is a property
      // of this fixture, not of hints, and asserting it would teach the wrong
      // thing.
      sameWork(
        facts.join(dims, col("product") === col("code")),
        facts.join(broadcast(dims), col("product") === col("code"))
      )
    }

    "and with it disabled, classic changes strategy while Sail does not" in {
      val key = "spark.sql.autoBroadcastJoinThreshold"
      val previous = spark.conf.get(key)
      try {
        spark.conf.set(key, "-1")
        val plain = Plans.shape(facts.join(dims, col("product") === col("code")))
        val hinted = Plans.shape(facts.join(broadcast(dims), col("product") === col("code")))

        perEngine {
          // The hint overrides the config: a sort-merge join becomes a
          // broadcast one. This is a real lever, and the only one in this spec.
          plain should include("SortMergeJoin")
          hinted should include("BroadcastHashJoin")
        } {
          // Sail's plan does not move. Not because the hint is refused — nothing
          // raises — but because it already collects the build side, and neither
          // the hint nor the threshold is consulted. So the advice is sound on
          // classic and inert here.
          hinted shouldBe plain
          plain should include("HashJoinExec")
        }
      } finally spark.conf.set(key, previous)
    }
  }

  "caching" - {

    "is accepted by both, and says nothing about whether it helps" in {
      // Included because its absence would be conspicuous, not because there is
      // advice to test: whether caching pays is a question about reuse and
      // memory, which no plan comparison can answer.
      val cached = facts.cache()
      try cached.count() shouldBe 3
      finally cached.unpersist()
    }
  }
}
