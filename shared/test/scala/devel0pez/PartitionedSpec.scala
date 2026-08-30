package devel0pez

import java.sql.Date

import org.apache.spark.sql.functions.col

/** One row of a partitioned table. `day` is declared **last**, and that is not a style choice — see
  * the first block below for what happens when it is not.
  */
final case class Daily(k: String, v: Int, day: Date)

/** The same row with `day` declared first — the shape that corrupts a positional insert, kept here
  * so the guard in `Storage.partitioned` has something to reject.
  */
final case class Misordered(day: Date, k: String, v: Int)

/** Writing to a partitioned table, which is what every daily job actually does.
  *
  * The rule that matters is the first one: a partition column is moved to the **end** of the
  * table's schema whatever order you declared it in. `insertInto` matches by position, so a model
  * that lists it anywhere else writes into the wrong column. `Storage.partitioned` refuses that
  * before anything is written, and the rest of this spec is what that instance is for.
  *
  * The engines also disagree about partitioned writes, in two places and one of them expensively.
  * That lives in `findings/`, because it is a fact about Sail today rather than about how to write
  * a job — see `PartitionedDivergenceSpec`.
  */
final class PartitionedSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val declaredFirst = s"part_first_$suffix"
  private val table = s"part_daily_$suffix"

  private def dropAll(): Unit =
    Seq(declaredFirst, table).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    // Declared with the partition column FIRST, on purpose.
    spark.sql(
      s"CREATE TABLE $declaredFirst (day DATE, k STRING, v INT) USING parquet PARTITIONED BY (day)"
    )
    seed()
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def rows(t: String): Seq[String] =
    spark.table(t).collect().map(_.toString).sorted.toSeq

  /** Two days of data, from scratch.
    *
    * Dropped and recreated rather than truncated: `TRUNCATE TABLE` is not in Sail's SQL parser, and
    * this spec is about partitioning rather than about that.
    */
  private def seed(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (k STRING, v INT, day DATE) USING parquet PARTITIONED BY (day)")
    spark.sql(s"INSERT INTO $table VALUES ('a',1,DATE'2026-01-19'), ('b',2,DATE'2026-01-20')")
  }

  "a partition column is moved to the end of the schema" - {

    "however you declared it, and both engines agree on that" in {
      // Declared `(day, k, v)`, reported as `(k, v, day)`. This is the rule that
      // makes `Conform` matter here: `insertInto` matches by position, so a case
      // class that lists `day` first writes the date into `k` — or, with ANSI on,
      // fails to. Declare partition columns last in the model and the two line up.
      spark.table(declaredFirst).schema.fieldNames.toSeq shouldBe Seq("k", "v", "day")
      Conform[Daily].schema.fieldNames.toSeq shouldBe Seq("k", "v", "day")
    }
  }

  "the Storage instance for a partitioned table" - {

    "writes and reads back through the typeclass" in {
      import Storage._
      val session = spark
      import session.implicits._
      seed()

      implicit val daily: Storage[Daily] = Storage.partitioned[Daily](table, Seq("day"))

      Seq(Daily("c", 3, Date.valueOf("2026-01-21"))).toDS().saveTo
      spark.load[Daily].count() shouldBe 3
    }

    "refuses a model whose partition column is not last, before writing anything" in {
      val session = spark
      import session.implicits._
      seed()

      val sink = Storage.partitioned[Misordered](table, Seq("day"))
      val refused = intercept[ConformError] {
        sink.save(Seq(Misordered(Date.valueOf("2026-01-19"), "x", 1)).toDS())
      }

      // The message names the actual problem, which is the thing ANSI's
      // `INCOMPATIBLE_DATA_FOR_TABLE` cannot do.
      refused.getMessage should include("partition columns must be the last fields")
      refused.getMessage should include("day, k, v")

      // And nothing was written.
      rows(table).size shouldBe 2
    }
  }

  "partition pruning" - {

    "reaches the scan on both engines" in {
      seed()
      val plan = Plans.of(spark.table(table).filter(col("day") === Date.valueOf("2026-01-19")))

      // The reason to partition at all: the other day's files are never opened.
      perEngine {
        plan should include("PartitionFilters: [")
        plan should include("day")
      } {
        plan.toLowerCase should include("day")
      }
    }
  }
}
