package devel0pez

import java.sql.{Date, Timestamp}

import Storage._

/** The `Storage` typeclass: where a `T` lives, injected rather than hardcoded.
  *
  * The test that carries the idea is the last one. It runs the very same ETL twice — once writing
  * to a catalogue table, once to an in-memory view — with nothing changed but which instance is in
  * scope, and compares the two results. If the job had any idea where its data lived, that test
  * could not be written.
  */
final class StorageSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val srcTbl = s"sales_src_$suffix"
  private val prodTbl = s"products_src_$suffix"
  private val outTbl = s"by_family_out_$suffix"
  private val outView = s"by_family_view_$suffix"

  private val audited = Timestamp.valueOf("2026-01-20 15:04:31")
  private val day = Date.valueOf("2026-01-20")

  private def dropAll(): Unit =
    Seq(srcTbl, prodTbl, outTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    // Wrong order for `Sale` and one column it does not want: the instance
    // conforms on the way in, so the job never sees this shape.
    spark.sql(s"""CREATE TABLE $srcTbl (
      day TIMESTAMP, amount DECIMAL(18,2), product STRING,
      branch STRING, country STRING, source_file STRING
    ) USING parquet""")
    spark.sql(s"CREATE TABLE $prodTbl (code STRING, name STRING, family STRING) USING parquet")
    spark.sql(s"""CREATE TABLE $outTbl (
      country STRING, branch STRING, family STRING,
      total DECIMAL(18,2), audited TIMESTAMP, day DATE
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $srcTbl VALUES
      (TIMESTAMP'2026-01-19 00:00:00', 100.50, 'P1', '0182', 'ES', 'a.csv'),
      (TIMESTAMP'2026-01-19 00:00:00', 200.25, 'P1', '0182', 'ES', 'a.csv'),
      (TIMESTAMP'2026-01-19 00:00:00',  33.33, 'P9', '0227', 'ES', 'b.csv')""")
    spark.sql(s"INSERT INTO $prodTbl VALUES ('P1','Widget','TOOLS'), ('P2','Gadget','TOOLS')")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private implicit val sales: Storage[Sale] = Storage.catalog[Sale](srcTbl)

  private def products = {
    val session = spark
    import session.implicits._
    spark.table(prodTbl).as[Product]
  }

  private def totals(ds: org.apache.spark.sql.Dataset[SalesByFamily]) =
    ds.collect().map(r => (r.branch, r.family) -> r.total).toMap

  "loading through the typeclass" - {

    "conforms on the way in, so the job never sees the table's shape" in {
      spark.load[Sale].schema.fieldNames.toSeq shouldBe
        Seq("country", "branch", "product", "amount", "day")
    }

    "reads the rows, not just the shape" in {
      spark.load[Sale].count() shouldBe 3
      spark.load[Sale].collect().head.country shouldBe "ES"
    }

    "is summoned by type, so the call site names no table" in {
      // `spark.load[Sale]` picked `sales` above out of implicit scope. Ask for
      // a type with no instance and it does not compile — which is the point.
      Storage[Sale] shouldBe theSameInstanceAs(sales)
    }
  }

  "the same ETL, two different destinations" - {

    "writes to a catalogue table when that instance is in scope" in {
      implicit val sink: Storage[SalesByFamily] = Storage.catalog[SalesByFamily](outTbl)
      val session = spark
      import session.implicits._

      ConformedEtl.runStored(spark, products, audited, day)

      val written = totals(spark.table(outTbl).as[SalesByFamily])
      written(("0182", "TOOLS")) shouldBe BigDecimal("300.75")
      written(("0227", "UNKNOWN")) shouldBe BigDecimal("33.33")
    }

    "writes to an in-memory view when that one is, with the job untouched" in {
      implicit val sink: Storage[SalesByFamily] = Storage.view[SalesByFamily](outView)
      val session = spark
      import session.implicits._

      // Byte for byte the same call as the test above. Only the instance changed.
      ConformedEtl.runStored(spark, products, audited, day)

      val written = totals(spark.table(outView).as[SalesByFamily])
      written(("0182", "TOOLS")) shouldBe BigDecimal("300.75")
      written(("0227", "UNKNOWN")) shouldBe BigDecimal("33.33")
    }

    "and both destinations hold the same thing" in {
      val session = spark
      import session.implicits._

      // Written by the two runs above. That these agree is what "the storage
      // is swappable" actually means, rather than a claim in a comment.
      totals(spark.table(outTbl).as[SalesByFamily]) shouldBe
        totals(spark.table(outView).as[SalesByFamily])
    }
  }
}
