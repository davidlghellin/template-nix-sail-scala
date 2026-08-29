package devel0pez

/** What each optimiser does with the shapes this project is written in.
  *
  * Two design decisions are on trial here, and both were made for readability rather than for
  * speed. Composing four stages with `andThen` puts a projection between each of them. Validating
  * *after* a join reads better than validating each side first, because only the joined row can
  * answer "does this reading have a price".
  *
  * The question is what those cost, and the answer differs by engine — which makes this the
  * clearest measurement in the project of how far apart the two optimisers currently are.
  */
final class OptimizerSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val readingsTbl = s"opt_readings_$suffix"
  private val tariffsTbl = s"opt_tariffs_$suffix"

  private def dropAll(): Unit =
    Seq(readingsTbl, tariffsTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(s"""CREATE TABLE $readingsTbl (
      meterId STRING, takenAt STRING, kwh STRING, tariff STRING
    ) USING parquet""")
    spark.sql(s"CREATE TABLE $tariffsTbl (code STRING, pricePerKwh STRING) USING parquet")
    spark.sql(s"""INSERT INTO $readingsTbl VALUES
      ('m1','2026-01-19 08:00:00','10.000','NOCTURNA'),
      ('m2','2026-01-19 09:00:00','5.000','FANTASMA')""")
    spark.sql(s"INSERT INTO $tariffsTbl VALUES ('NOCTURNA','0.100000')")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def readings = BillingEtl.readReadings(spark, readingsTbl)
  private def tariffs = BillingEtl.readTariffs(spark, tariffsTbl).transform(BillingEtl.parseTariff)

  private def count(plan: String, node: String): Int = node.r.findAllIn(plan).size

  "stacking stages with andThen" - {

    "costs nothing on classic, which collapses them into one projection" in {
      val one = Plans.of(readings.transform(MeterEtl.parse))
      val three = Plans.of(
        readings
          .transform(MeterEtl.parse)
          .transform(MeterEtl.validate)
          .transform(MeterEtl.daily)
      )

      perEngine {
        // Three stages, three `select`s, and `CollapseProject` leaves one. The
        // composed design is free at execution time.
        count(one, "Project") shouldBe 1
        count(three, "Project") shouldBe 1
      } {
        // Sail keeps them. Projections are cheap — no shuffle, no IO — so this
        // is CPU rather than an algorithmic cost, but it is not nothing, and it
        // is the kind of rule that arrives late in an optimiser's life.
        count(one, "ProjectionExec") shouldBe 1
        count(three, "ProjectionExec") should be > 1
      }
    }
  }

  "validating after a join" - {

    "gets the left-hand checks pushed below the join on both engines" in {
      val plan = Plans.of(
        readings
          .transform(MeterEtl.parse)
          .transform(BillingEtl.pricedWith(tariffs))
          .transform(BillingEtl.validate)
      )

      // Whatever `validate` can decide from the reading alone is applied before
      // the join, on both. Writing the checks after the join therefore does not
      // mean running them after it.
      perEngine {
        plan should include("PushedFilters: [IsNotNull(takenAt), IsNotNull(kwh)]")
      } {
        plan should include("btrim(meterId@0) != ")
      }
    }

    "but only classic turns the LEFT join into an inner one" in {
      val plan = Plans.of(
        readings
          .transform(MeterEtl.parse)
          .transform(BillingEtl.pricedWith(tariffs))
          .transform(BillingEtl.validate)
      )

      perEngine {
        // `validate` requires `pricePerKwh` to be present, so every row the LEFT
        // join invents is discarded immediately — which makes the join an inner
        // one. Catalyst spots that and rewrites it, and pushes the null check
        // onto the tariff side too, where it was never written.
        plan should include("Inner")
        plan should not include "LeftOuter"
      } {
        // Sail keeps the outer join and filters afterwards: it materialises the
        // null-extended rows and then throws them away. On a fact table with
        // many unmatched rows that is real work, and it is the single widest
        // optimiser gap this project has measured.
        plan should include("join_type=Left")
        plan should include("FilterExec: #11@4 IS NOT NULL")
      }
    }
  }
}
