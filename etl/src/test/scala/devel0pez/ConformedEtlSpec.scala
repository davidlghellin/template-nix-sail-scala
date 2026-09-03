package devel0pez

import java.sql.{Date, Timestamp}

import Conform._

/** The ETL end to end with both boundaries guarded.
  *
  * The source table is deliberately hostile: its columns are in an order nobody would choose, and
  * it carries one the model does not want. That is not a contrived case — it is what a table you do
  * not own looks like.
  */
final class ConformedEtlSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val srcTbl = s"sales_wide_$suffix"
  private val shortTbl = s"sales_short_$suffix"
  private val prodTbl = s"products_conf_$suffix"
  private val targetTbl = s"by_family_conf_$suffix"

  private val audited = Timestamp.valueOf("2026-01-20 15:04:31")
  private val day = Date.valueOf("2026-01-20")

  private def dropAll(): Unit =
    Seq(srcTbl, shortTbl, prodTbl, targetTbl)
      .foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()

    // Columns in the wrong order for `Sale`, plus one it does not declare.
    spark.sql(s"""CREATE TABLE $srcTbl (
      day TIMESTAMP, amount DECIMAL(18,2), product STRING,
      branch STRING, country STRING, ingested_at STRING
    ) USING parquet""")

    // The same source minus a column the model needs.
    spark.sql(s"""CREATE TABLE $shortTbl (
      day TIMESTAMP, product STRING, branch STRING, country STRING
    ) USING parquet""")

    spark.sql(s"CREATE TABLE $prodTbl (code STRING, name STRING, family STRING) USING parquet")

    // Declared in the order `SalesByFamily` declares its fields: that is the
    // contract `conformTo` upholds, and `insertInto` matches by position.
    spark.sql(s"""CREATE TABLE $targetTbl (
      country STRING, branch STRING, family STRING,
      total DECIMAL(18,2), audited TIMESTAMP, day DATE
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $srcTbl VALUES
      (TIMESTAMP'2026-01-19 00:00:00', 100.50, 'P1', '0182', 'ES', 'batch-7'),
      (TIMESTAMP'2026-01-19 00:00:00', 200.25, 'P1', '0182', 'ES', 'batch-7'),
      (TIMESTAMP'2026-01-19 00:00:00',  33.33, 'P9', '0227', 'ES', 'batch-7')""")
    spark.sql(s"INSERT INTO $prodTbl VALUES ('P1','Widget','TOOLS'), ('P2','Gadget','TOOLS')")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def products = {
    val session = spark
    import session.implicits._
    spark.table(prodTbl).as[Product]
  }

  "the read boundary" - {

    "as[Sale] would keep the table's shape, wrong order and stray column included" in {
      val session = spark
      import session.implicits._

      // Nothing raises. This is the `Dataset[Sale]` most code ends up holding.
      spark.table(srcTbl).as[Sale].schema.fieldNames.toSeq shouldBe
        Seq("day", "amount", "product", "branch", "country", "ingested_at")
    }

    "conformTo[Sale] makes it the shape the model declares" in {
      ConformedEtl.read(spark, srcTbl).schema.fieldNames.toSeq shouldBe
        Seq("country", "branch", "product", "amount", "day")
    }

    "and the values follow the names, not the positions" in {
      val first = ConformedEtl.read(spark, srcTbl).collect().head

      first.country shouldBe "ES"
      first.branch shouldBe "0182"
      first.product should startWith("P")
    }

    "fails at the boundary when the source is missing a column" in {
      // Before a single row is read: no session work, no half-written table.
      val error = intercept[ConformError](ConformedEtl.read(spark, shortTbl))

      error.getMessage should include("missing columns: amount")
    }
  }

  "the whole run" - {

    "lands the right values in the target table" in {
      val session = spark
      import session.implicits._

      ConformedEtl.run(spark, srcTbl, products, targetTbl, audited, day)

      val rows = spark
        .table(targetTbl)
        .as[SalesByFamily]
        .collect()
        .map(r => (r.branch, r.family) -> r)
        .toMap

      // 100.50 + 200.25, both P1 at branch 0182.
      rows(("0182", "TOOLS")).total shouldBe BigDecimal("300.75")
      // P9 is not in the catalogue and still comes out.
      rows(("0227", "UNKNOWN")).total shouldBe BigDecimal("33.33")
      // Read back from a real table: country did not end up holding the family.
      rows(("0182", "TOOLS")).country shouldBe "ES"
      rows(("0182", "TOOLS")).audited shouldBe audited
      rows(("0182", "TOOLS")).day shouldBe day
    }

    "writes the columns in the order the case class declares" in {
      val written = ConformedEtl
        .read(spark, srcTbl)
        .transform(ConformedEtl.stages(products, audited, day))
        .conformTo[SalesByFamily]

      written.schema.fieldNames.toSeq shouldBe
        Seq("country", "branch", "family", "total", "audited", "day")
    }
  }
}
