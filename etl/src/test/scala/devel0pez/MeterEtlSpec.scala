package devel0pez

import org.apache.spark.sql.Encoders

import Conform._
import Storage._

/** The reference ETL, exercised the way the reference ought to be.
  *
  * The landing table carries rows that cannot survive, one per rejection reason, because a pipeline
  * only ever shown the happy path proves nothing about the branch that matters at 3am.
  */
final class MeterEtlSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val rawTbl = s"readings_raw_$suffix"
  private val usageTbl = s"usage_daily_$suffix"
  private val deadTbl = s"readings_rejected_$suffix"

  private def dropAll(): Unit =
    Seq(rawTbl, usageTbl, deadTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(s"""CREATE TABLE $rawTbl (
      meterId STRING, takenAt STRING, kwh STRING, tariff STRING
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $usageTbl (
      meterId STRING, day DATE, tariff STRING, kwh DECIMAL(18,3), readings BIGINT
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $deadTbl (
      meterId STRING, takenAt STRING, kwh STRING, reason STRING
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $rawTbl VALUES
      ('m1','2026-01-19 08:00:00','1.500','NOCTURNA'),
      ('m1','2026-01-19 20:00:00','2.250','NOCTURNA'),
      ('m2','2026-01-19 09:00:00','0.750',''),
      ('m2','2026-01-19 21:00:00','1.250',NULL),
      ('','2026-01-19 10:00:00','1.000','NOCTURNA'),
      ('m3','ayer','1.000','NOCTURNA'),
      ('m3','2026-01-19 11:00:00','no-va','NOCTURNA'),
      ('m3','2026-01-19 12:00:00','-5.000','NOCTURNA')""")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private implicit val readings: Storage[RawReading] = Storage.catalog[RawReading](rawTbl)
  private implicit val usage: Storage[DailyUsage] = Storage.catalog[DailyUsage](usageTbl)
  private implicit val dead: Storage[RejectedReading] = Storage.catalog[RejectedReading](deadTbl)

  private def parsed = spark.load[RawReading].transform(MeterEtl.parse)

  "parsing" - {

    "turns text into types without letting one bad row kill the run" in {
      val rows = parsed.collect()

      rows.length shouldBe 8
      rows.count(_.takenAt == null) shouldBe 1 // 'ayer'
      rows.count(_.kwh == null) shouldBe 1 // 'no-va'
    }

    "treats an empty tariff and a missing one as the same absence" in {
      // m2 has one row with '' and one with NULL; both must read as None.
      parsed.collect().filter(_.meterId == "m2").map(_.tariff).toSeq shouldBe Seq(None, None)
    }
  }

  "validation" - {

    "partitions the rows: none in both, none in neither" in {
      val kept = parsed.transform(MeterEtl.validate).count()
      val dropped = parsed.transform(MeterEtl.rejected).count()

      kept shouldBe 4
      dropped shouldBe 4
      kept + dropped shouldBe parsed.count()
    }

    "says which check rejected each row" in {
      parsed.transform(MeterEtl.rejected).collect().map(_.reason).toSet shouldBe Set(
        "meter id is empty",
        "takenAt is not a timestamp",
        "kwh is not a number",
        "kwh is negative"
      )
    }
  }

  "the aggregate" - {

    "groups by meter, day and tariff, and closes the Option" in {
      val rows = parsed
        .transform(MeterEtl.validate)
        .transform(MeterEtl.daily)
        .collect()
        .map(u => (u.meterId, u.tariff) -> u)
        .toMap

      rows(("m1", "NOCTURNA")).kwh shouldBe BigDecimal("3.750")
      rows(("m1", "NOCTURNA")).readings shouldBe 2L
      // Both of m2's rows had no tariff, so they group together under UNKNOWN.
      rows(("m2", "UNKNOWN")).kwh shouldBe BigDecimal("2.000")
      rows(("m2", "UNKNOWN")).readings shouldBe 2L
    }
  }

  "the shape of the types" - {

    "shows that Option buys nothing at the schema level" in {
      val nullable = Encoders.product[Reading].schema.fields.map(f => f.name -> f.nullable).toMap

      // `tariff` is Option and `kwh` is not, and the schema cannot tell them
      // apart: both nullable. What makes `kwh` non-null is `validate`, not its
      // type. `Option` is an obligation on the Scala side, not a constraint on
      // the data.
      nullable("tariff") shouldBe true
      nullable("kwh") shouldBe true
    }
  }

  "a stage on its own" - {

    "is a plain function, testable without a table" in {
      val session = spark
      import session.implicits._
      val one = Seq(
        Reading("m9", java.sql.Timestamp.valueOf("2026-02-01 10:00:00"), BigDecimal("5.000"), None)
      ).toDS()

      val out = one.transform(MeterEtl.daily).collect().head

      out.tariff shouldBe "UNKNOWN"
      out.day shouldBe java.sql.Date.valueOf("2026-02-01")
    }
  }

  "the whole job" - {

    "writes both branches where the injected storage says" in {
      val session = spark
      import session.implicits._

      // No table names in this call. Three implicits above decide everything.
      MeterEtl.run(spark)

      val written = spark
        .table(usageTbl)
        .as[DailyUsage]
        .collect()
        .map(u => (u.meterId, u.tariff) -> u.kwh)
        .toMap
      written(("m1", "NOCTURNA")) shouldBe BigDecimal("3.750")
      written(("m2", "UNKNOWN")) shouldBe BigDecimal("2.000")

      spark.table(deadTbl).as[RejectedReading].count() shouldBe 4
    }

    "reads back through the same typeclass that wrote it" in {
      // The round trip is where a primitive field bites: `readings: Long` is
      // `nullable = false` in the encoder, while every column read back from a
      // table is nullable. `Conform` relaxes the target for exactly this.
      val back = spark.load[DailyUsage]

      back.schema.fieldNames.toSeq shouldBe Seq("meterId", "day", "tariff", "kwh", "readings")
      back
        .collect()
        .map(u => (u.meterId, u.tariff) -> u.readings)
        .toMap
        .apply(("m1", "NOCTURNA")) shouldBe 2L
    }
  }
}
