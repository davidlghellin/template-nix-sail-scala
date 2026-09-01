package devel0pez

import Conform._
import org.apache.spark.sql.Dataset

/** Two landing tables, joined mid-chain, billed.
  *
  * The fixtures carry one row per way of failing, and the rejection reasons are the point: after a
  * join there are two new ones that neither source could have detected on its own — a reading with
  * no tariff at all, and a reading whose tariff is not in the catalogue.
  */
final class BillingEtlSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val readingsTbl = s"bill_readings_$suffix"
  private val tariffsTbl = s"bill_tariffs_$suffix"
  private val billsTbl = s"bill_out_$suffix"
  private val deadTbl = s"bill_rejected_$suffix"

  private def dropAll(): Unit =
    Seq(readingsTbl, tariffsTbl, billsTbl, deadTbl)
      .foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(s"""CREATE TABLE $readingsTbl (
      meterId STRING, takenAt STRING, kwh STRING, tariff STRING
    ) USING parquet""")
    spark.sql(s"CREATE TABLE $tariffsTbl (code STRING, pricePerKwh STRING) USING parquet")
    spark.sql(s"""CREATE TABLE $billsTbl (
      meterId STRING, day DATE, tariff STRING, kwh DECIMAL(18,3), cost DECIMAL(18,2)
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $deadTbl (
      meterId STRING, takenAt STRING, kwh STRING, reason STRING
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $readingsTbl VALUES
      ('m1','2026-01-19 08:00:00','10.000','NOCTURNA'),
      ('m1','2026-01-19 20:00:00','20.000','NOCTURNA'),
      ('m2','2026-01-19 09:00:00','5.000','DIURNA'),
      ('m3','2026-01-19 10:00:00','1.000',NULL),
      ('m4','2026-01-19 11:00:00','1.000','FANTASMA'),
      ('m5','no-es-fecha','1.000','NOCTURNA')""")
    spark.sql(s"""INSERT INTO $tariffsTbl VALUES
      ('NOCTURNA','0.100000'), ('DIURNA','0.250000')""")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def tariffs: Dataset[ParsedTariff] =
    BillingEtl.readTariffs(spark, tariffsTbl).transform(BillingEtl.parseTariff)

  private def priced: Dataset[PricedReading] =
    BillingEtl
      .readReadings(spark, readingsTbl)
      .transform(MeterEtl.parse)
      .transform(BillingEtl.pricedWith(tariffs))

  "the two branches" - {

    "are parsed independently before they meet" in {
      tariffs
        .collect()
        .map(t => t.code -> t.pricePerKwh)
        .toMap
        .apply("NOCTURNA") shouldBe Some(BigDecimal("0.100000"))
    }

    "and the join keeps every reading, priced or not" in {
      // A LEFT join on purpose: the rows with no usable tariff have to reach
      // `validate`, which is what decides their fate and names it.
      priced.count() shouldBe 6

      // Two rows come out unpriced: m3 carries no tariff, m4 carries one the
      // catalogue does not have. Note this is *not* the number of rows that get
      // rejected — m5 is priced perfectly well and still fails, on a timestamp
      // that never parsed. Having a price and being billable are different
      // questions, and only `validate` asks the second one.
      priced.collect().count(_.pricePerKwh.isEmpty) shouldBe 2
    }
  }

  "validation after the join" - {

    "can ask questions neither source could answer alone" in {
      val reasons = priced.transform(BillingEtl.rejected).collect().map(_.reason).toSet

      // The first two exist in `MeterEtl` too. The last two are new, and only
      // exist because there is now a second table to be absent from.
      reasons should contain("takenAt is not a timestamp")
      reasons should contain("no tariff on the reading")
      reasons should contain("tariff is not in the catalogue")
    }

    "splits the rows with none in both and none in neither" in {
      val billable = priced.transform(BillingEtl.validate).count()
      val dropped = priced.transform(BillingEtl.rejected).count()

      billable shouldBe 3
      dropped shouldBe 3
      billable + dropped shouldBe priced.count()
    }
  }

  "the bill" - {

    "multiplies each reading by its own tariff before summing" in {
      val bills = priced
        .transform(BillingEtl.validate)
        .transform(BillingEtl.bill)
        .collect()
        .map(b => (b.meterId, b.tariff) -> b)
        .toMap

      // m1: (10 + 20) kWh at 0.10
      bills(("m1", "NOCTURNA")).kwh shouldBe BigDecimal("30.000")
      bills(("m1", "NOCTURNA")).cost shouldBe BigDecimal("3.00")
      // m2: 5 kWh at 0.25 — a different price, so the multiply has to happen
      // per row rather than after the sum.
      bills(("m2", "DIURNA")).cost shouldBe BigDecimal("1.25")
    }
  }

  "the composed pipeline" - {

    "is the same as running the stages one at a time" in {
      val composed = BillingEtl
        .readReadings(spark, readingsTbl)
        .transform(BillingEtl.pipeline(tariffs))

      val chained = priced.transform(BillingEtl.validate).transform(BillingEtl.bill)

      composed.collect().toSeq should contain theSameElementsAs chained.collect().toSeq
    }

    "writes both tables end to end" in {
      val session = spark
      import session.implicits._

      BillingEtl.run(spark, readingsTbl, tariffsTbl, billsTbl, deadTbl)

      spark.table(billsTbl).as[Bill].count() shouldBe 2
      spark.table(deadTbl).as[RejectedReading].count() shouldBe 3
      spark.table(billsTbl).conformTo[Bill].schema.fieldNames.toSeq shouldBe
        Seq("meterId", "day", "tariff", "kwh", "cost")
    }
  }
}
