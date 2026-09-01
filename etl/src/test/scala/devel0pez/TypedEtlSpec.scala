package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType

/** A typed ETL end to end: read a table as a `Dataset[T]`, join, aggregate, and insert into another
  * table — with case classes at every step.
  *
  * The point is not the ETL, it is the question underneath: how much of the typed API survives
  * Spark Connect. A `DataFrame` is a `Dataset[Row]`, so the hope is that everything does. It does
  * not, and the line is worth knowing before building on it — see `TypedEtl`, and `TypedLambdaSpec`
  * for what exactly fails and the two ways round it.
  */
final class TypedEtlSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val salesTbl = s"sales_$suffix"
  private val prodTbl = s"products_$suffix"
  private val targetTbl = s"sales_by_family_$suffix"

  private val audited = Timestamp.valueOf("2026-01-20 15:04:31")
  private val day = Date.valueOf("2026-01-20")

  private def dropAll(): Unit =
    Seq(salesTbl, prodTbl, targetTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(s"""CREATE TABLE $salesTbl (
      country STRING, branch STRING, product STRING, amount DECIMAL(18,2), day TIMESTAMP
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $prodTbl (
      code STRING, name STRING, family STRING
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $targetTbl (
      country STRING, branch STRING, family STRING,
      total DECIMAL(18,2), audited TIMESTAMP, day DATE
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $salesTbl VALUES
      ('ES','0182','P1',100.50, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0182','P1',200.25, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0182','P2', 10.00, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0227','P9', 33.33, TIMESTAMP'2026-01-19 00:00:00')""")
    spark.sql(s"""INSERT INTO $prodTbl VALUES
      ('P1','Widget','TOOLS'), ('P2','Gadget','TOOLS')""")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  "a typed ETL" - {

    "reads the source table as a Dataset of case classes" in {
      val sales = TypedEtl.sales(spark, salesTbl)

      sales.count() shouldBe 4
      sales.collect().head shouldBe a[Sale]
      sales.schema.fieldNames.toSeq shouldBe Seq("country", "branch", "product", "amount", "day")
    }

    "reaches its columns by name of field, not by string" in {
      val sales = TypedEtl.sales(spark, salesTbl)

      // Every handle in `Sale.Cols` exercised at once. Rename a field in the case
      // class and forget the handle and this fails here, loudly, instead of
      // surviving to an AnalysisException in the middle of an ETL. The compiler
      // already covers the other direction: `sales.produkt` does not build.
      val selected =
        sales.select(sales.country, sales.branch, sales.product, sales.amount, sales.day)

      selected.schema.fieldNames.toSeq shouldBe Encoders.product[Sale].schema.fieldNames.toSeq
    }

    "casts through a handle like any other column" in {
      val sales = TypedEtl.sales(spark, salesTbl)

      // A handle is a plain `Column`, so nothing in the column API is lost by
      // going through it: here the branch code, a String in the source table,
      // read back as an Int. Worth asserting rather than assuming — a cast is
      // evaluated by the engine, so it is exactly the kind of thing the two
      // backends could disagree on.
      val branches =
        sales.select(sales.branch.cast(IntegerType)).collect().map(_.getInt(0)).toSet

      branches shouldBe Set(182, 227)
    }

    "raises on a cast that cannot succeed, and try_cast is the tolerant form" in {
      val sales = TypedEtl.sales(spark, salesTbl)

      // `product` holds "P1", "P2", "P9" — nothing that parses as a number. The
      // suite runs with ANSI mode on (see `SparkSuite`), so that is an error and
      // not a quiet NULL, which is the whole point of the flag.
      val failed =
        intercept[Exception](sales.select(sales.product.cast(IntegerType)).collect())

      // Both engines refuse it, so ANSI semantics agree. What they do not agree
      // on is the *identity* of the error: classic Spark raises its own error
      // class, while Sail fails on the Rust side with something Connect cannot
      // map to one, so it arrives wrapped as
      // `CONNECT_CLIENT_UNEXPECTED_MISSING_SQL_STATE`. Asserted per backend
      // rather than loosely: the day Sail reports the Spark error class, this
      // test says so instead of quietly passing.
      perEngine {
        failed.getMessage should include("CAST_INVALID_INPUT")
      } {
        failed.getMessage should include("Cannot cast string 'P1'")
      }

      // `try_cast` is a method on `Column`, so it composes with a handle the
      // same way `cast` does, and returns NULL rather than raising.
      sales
        .select(sales.product.try_cast(IntegerType))
        .collect()
        .forall(_.isNullAt(0)) shouldBe true
    }

    "joins against the catalogue, keeping unmatched rows" in {
      val enriched = TypedEtl.enrich(TypedEtl.sales(spark, salesTbl), products)

      val families = enriched.collect().map(e => e.product -> e.family).toMap
      families("P1") shouldBe "TOOLS"
      // P9 is not in the catalogue and must survive the left join.
      families("P9") shouldBe "UNKNOWN"
    }

    "aggregates into the target case class" in {
      val result = byFamily.collect().map(r => (r.branch, r.family) -> r).toMap

      result(("0182", "TOOLS")).total shouldBe BigDecimal("310.75")
      result(("0227", "UNKNOWN")).total shouldBe BigDecimal("33.33")
      result(("0182", "TOOLS")).audited shouldBe audited
      result(("0182", "TOOLS")).day shouldBe day
    }

    "inserts the typed result into the target table" in {
      byFamily.write.insertInto(targetTbl)

      val written = spark.table(targetTbl)
      written.count() shouldBe 2
      // Read back as the same case class: the round trip keeps the types.
      val back = {
        val session = spark // `import spark.implicits._` needs a stable identifier
        import session.implicits._
        written.as[SalesByFamily].collect()
      }
      back.map(_.total).sum shouldBe BigDecimal("344.08")
      back.head.day shouldBe day
    }
  }

  private def products = {
    val session = spark
    import session.implicits._
    spark.table(prodTbl).as[Product]
  }

  private def byFamily =
    TypedEtl.byFamily(TypedEtl.enrich(TypedEtl.sales(spark, salesTbl), products), audited, day)
}
