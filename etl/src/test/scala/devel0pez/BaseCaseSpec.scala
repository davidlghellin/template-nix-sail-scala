package devel0pez

import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{DataFrame, Row}

import devel0pez.BaseCase._

/** The compatibility base case: the same assertions as the Python template.
  *
  * The expected values are what real Spark 4.x produces, not what one would guess it should.
  */
final class BaseCaseSpec extends SparkSuite {

  private def t1: DataFrame = spark.createDataFrame(Rows1.asJava, Table1)
  private def t2: DataFrame = spark.createDataFrame(Rows2.asJava, Table2)

  private def byKey(df: DataFrame, column: String = "OUT_COL_3"): Map[String, Row] =
    df.collect().map(row => row.getAs[String](column) -> row).toMap

  "the expressions" - {

    "filter, normalise and deduplicate" in {
      val rows = filterAndDeduplicate(t2, Cutoff).collect()

      // 0227 -> 0182 leaves two identical rows that DISTINCT collapses; C9 is
      // AUT and gets filtered out by type.
      rows.length shouldBe 1
      rows.head.getAs[String]("TABLE_2_COL_2") shouldBe "0182"
      rows.head.getAs[String]("TABLE_2_COL_4") shouldBe "P1"
    }

    "the left join keeps the unmatched row" in {
      val rows = byKey(joinAndAggregate(t1, filterAndDeduplicate(t2, Cutoff), Audit))

      rows("C1").getAs[String]("OUT_COL_4") shouldBe "P1"
      rows("C2").getAs[String]("OUT_COL_4") shouldBe "NO_MATCH"
    }

    "the aggregate keeps the decimal scale" in {
      val rows = byKey(joinAndAggregate(t1, filterAndDeduplicate(t2, Cutoff), Audit))

      rows("C1").getAs[BigDecimal]("OUT_COL_5") shouldBe new BigDecimal("300.75")
      rows("C2").getAs[BigDecimal]("OUT_COL_5") shouldBe new BigDecimal("10.00")
    }
  }

  "conform" - {

    "orders and casts to the target schema" in {
      val output = etl(t1, t2, Cutoff, Audit)

      output.columns.toSeq shouldBe TableOut.fields.map(_.name).toSeq
      output.schema.fields.map(_.dataType).toSeq shouldBe TableOut.fields.map(_.dataType).toSeq

      val rows = byKey(output)
      // The timestamp -> date cast does not shift the day.
      rows("C1").getAs[Date]("OUT_COL_7") shouldBe Date.valueOf("2024-12-31")
      rows("C1").getAs[Timestamp]("OUT_COL_6") shouldBe Timestamp.valueOf("2025-01-20 15:04:31")
    }

    "detects missing columns" in {
      val error = intercept[IllegalArgumentException](conform(t1, TableOut))

      error.getMessage should include("missing columns")
    }
  }

  "nullability of the output" - {

    "tells apart what cannot be null" in {
      // coalesce and lit cannot be null; the rest comes from the source tables.
      val nullability =
        etl(t1, t2, Cutoff, Audit).schema.fields.map(f => f.name -> f.nullable).toMap

      nullability("OUT_COL_4") shouldBe false
      nullability("OUT_COL_6") shouldBe false
      nullability("OUT_COL_1") shouldBe true
      nullability("OUT_COL_5") shouldBe true
    }
  }

  "a join with repeated column names" - {

    "is qualified by DataFrame" in {
      val schema = "COL_A string, COL_B string"
      // Two keys a side, and that is what makes this a test. With one row each,
      // a degenerate condition — the `COL_A == COL_A` that the Connect client
      // warns about — produces the same single row as a correct join, so the
      // assertion would hold either way. Two keys tell them apart: qualified
      // gives 2 rows, trivially true gives 4.
      val a = spark.createDataFrame(
        Seq(Row("k1", "left1"), Row("k2", "left2")).asJava,
        structTypeOf(schema)
      )
      val b = spark.createDataFrame(
        Seq(Row("k1", "right1"), Row("k2", "right2")).asJava,
        structTypeOf(schema)
      )

      val joined = a
        .join(b, b("COL_A") === a("COL_A"), "inner")
        .select(
          a("COL_A").as("KEY"),
          a("COL_B").as("LEFT_COL"),
          b("COL_B").as("RIGHT_COL")
        )

      joined.count() shouldBe 2

      val rows = joined
        .collect()
        .map(r =>
          r.getAs[String]("KEY") -> (r.getAs[String]("LEFT_COL"), r.getAs[String]("RIGHT_COL"))
        )
        .toMap

      rows("k1") shouldBe ("left1", "right1")
      rows("k2") shouldBe ("left2", "right2")
    }
  }

  private def structTypeOf(ddl: String) = org.apache.spark.sql.types.StructType.fromDDL(ddl)
}
