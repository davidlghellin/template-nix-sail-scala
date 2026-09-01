package devel0pez

import org.apache.spark.sql.functions.{array, col, explode, sum}

/** What a typed lambda costs, measured in the plan rather than argued about.
  *
  * The headline everywhere else is that Sail refuses `ds.filter(_.amount > 50)` and classic runs
  * it. That framing makes Sail look like the poor relation. This spec measures what classic
  * actually *does* with it, and the framing does not survive: the lambda reads every column of the
  * file and pushes no filter into it, so classic is not being generous — it is quietly handing you
  * the slow path.
  *
  * The table is parquet and five columns wide on purpose. Pushdown and column pruning only show up
  * in a plan when there is a file underneath and columns to leave unread.
  */
final class PushdownSpec extends SparkSuite {

  private val table = s"pushdown_${backend.replace('-', '_')}"

  private def dropTable(): Unit = spark.sql(s"DROP TABLE IF EXISTS $table")

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTable()
    spark.sql(s"""CREATE TABLE $table (
      country STRING, branch STRING, product STRING, amount DECIMAL(18,2), day TIMESTAMP
    ) USING parquet""")
    spark.sql(s"""INSERT INTO $table VALUES
      ('ES','0182','P1',100.50, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0227','P9', 33.33, TIMESTAMP'2026-01-19 00:00:00')""")
  }

  override def afterAll(): Unit =
    try dropTable()
    finally super.afterAll()

  private def sales = {
    val session = spark
    import session.implicits._
    spark.table(table).as[Sale]
  }

  /** Both engines name the scan's columns in the plan; only the words differ. */
  private def scanReads(plan: String, column: String): Boolean =
    plan.contains(s"$column:") || plan.contains(s"[$column,") || plan.contains(s" $column,") ||
      plan.contains(s"[$column]") || plan.contains(s"$column@")

  "the column form" - {

    "pushes the filter into the file and reads only the columns it needs" in {
      val plan = Plans.of(sales.filter(col("amount") > 50).select(col("branch")))

      perEngine {
        plan should include("GreaterThan(amount,50.00)")
        plan should include("ReadSchema: struct<branch:string,amount:decimal(18,2)>")
      } {
        // DataFusion says the same thing in its own words, and says one more:
        // `pruning_predicate` is row-group skipping from the parquet statistics,
        // which Catalyst also does but does not write down.
        plan should include("predicate=amount")
        plan should include("projection=[branch, amount]")
        plan should include("pruning_predicate=")
      }

      // Either way: `country`, `product` and `day` are never read.
      scanReads(plan, "country") shouldBe false
      scanReads(plan, "product") shouldBe false
    }
  }

  "the typed lambda" - {

    "costs both the pushdown and the pruning, and classic never says so" in {
      perEngine {
        val plan = Plans.of(sales.filter(_.amount > 50).select(col("branch")))

        // Nothing reaches the file. Rows are read in order to be thrown away.
        plan should include("PushedFilters: []")

        // And every column is read to satisfy a lambda that touches one. On two
        // rows this is free; on a billion it is the difference between reading
        // 200 GB and reading 12.
        scanReads(plan, "country") shouldBe true
        scanReads(plan, "product") shouldBe true
        scanReads(plan, "day") shouldBe true
      } {
        // On Sail it does not run at all, and the error names neither the lambda
        // nor the reason — see `TypedEtlSpec` for why, and for the one-line fix
        // in Sail's own resolver.
        //
        // The refactor this failure forces is the same refactor classic wanted
        // all along. Rewriting the predicate as a column is not merely how you
        // make it work here: it is also what recovers the pushdown and the
        // pruning that the lambda was costing on the engine where it ran. That
        // is why implementing Scala UDFs in Sail is not on this project's wish
        // list — it would be months of work to deliver the slower path.
        val refused = intercept[Exception](Plans.of(sales.filter(_.amount > 50)))

        refused.getMessage should include("wildcard with plan ID")
      }
    }
  }

  "every other closure Sail refuses" - {

    // `ClosureSpec` establishes that `groupByKey`, `flatMap` and `map` all fail
    // on Sail. What that spec cannot say is whether refusing them costs anything
    // real. It does not — measured here, each of them gives up column pruning on
    // the engine where it *does* run, exactly as `filter` does.

    "gives up column pruning too, on classic" in {
      val session = spark
      import session.implicits._

      perEngine {
        val byKey = Plans.of(sales.groupByKey(_.country).count())
        val flattened = Plans.of(sales.flatMap(s => Seq(s.product)))
        val mapped = Plans.of(sales.map(_.amount))

        // Each reads the whole row, because the closure might. `groupByKey`
        // needs one column and reads five; `flatMap` needs one and reads five;
        // `map(_.amount)` needs one and reads five. `day` is the witness: no
        // closure here mentions it and every one of them loads it.
        scanReads(byKey, "day") shouldBe true
        scanReads(flattened, "day") shouldBe true
        scanReads(mapped, "day") shouldBe true
      } {
        // They do not run here at all, which `ClosureSpec` pins. The point of
        // this spec is what the column form below buys, and that works on both.
        succeed
      }
    }

    "while the column equivalent reads one column, on both engines" in {
      // This is the sentence the whole spec exists to earn: *it does not work,
      // and rewriting it so that it does also makes it faster*. The refactor
      // Sail forces is not a tax for using Sail — it is the optimisation
      // classic never told you that you were missing.
      val grouped = Plans.of(sales.groupBy(col("country")).count())
      val exploded = Plans.of(sales.select(explode(array(col("product")))))
      val summed = Plans.of(sales.select(sum(col("amount"))))

      // The same three questions, asked in a form the planner can see into.
      scanReads(grouped, "day") shouldBe false
      scanReads(exploded, "day") shouldBe false
      scanReads(summed, "day") shouldBe false
    }
  }

  "the macro" - {

    "gets the pushdown back, on the engine where the lambda still ran" in {
      // Worth stating plainly, because it reframes the spike: `filterExpr` was
      // built so Dataset-shaped code would run on Sail. It also makes that code
      // faster on classic, where the lambda worked and quietly did not.
      import devel0pez.macros.Expr._
      val plan = Plans.of(sales.filterExpr(_.amount > 50).select(col("branch")))

      perEngine {
        plan should include("GreaterThan(amount,50.00)")
      } {
        plan should include("predicate=amount")
      }

      scanReads(plan, "product") shouldBe false
    }
  }
}
