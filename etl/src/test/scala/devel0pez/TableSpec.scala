package devel0pez

import scala.jdk.CollectionConverters._

/** An ETL end to end: create a table, load it, transform, and insert.
  *
  * Every other spec stops at the DataFrame, which is where everything works. The real problems show
  * up on write: the positional order of the columns, the types of the target schema, the write
  * mode, the partition. `BaseCase.conform` exists for exactly that, and until now nothing checked
  * that what it produces actually goes into a table.
  */
final class TableSpec extends SparkSuite {

  private val table = s"salida_${backend.replace('-', '_')}"

  private def dropTable(): Unit = spark.sql(s"DROP TABLE IF EXISTS $table")

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropTable()
    // The target schema is declared in SQL rather than inferred from the data:
    // that is what gives `insertInto` something to match against.
    spark.sql(s"""
      CREATE TABLE $table (
        OUT_COL_1 STRING, OUT_COL_2 STRING, OUT_COL_3 STRING, OUT_COL_4 STRING,
        OUT_COL_5 DECIMAL(18,2), OUT_COL_6 TIMESTAMP, OUT_COL_7 DATE
      ) USING parquet
    """)
  }

  override def afterAll(): Unit =
    try dropTable()
    finally super.afterAll()

  "the target table" - {

    "accepts the conformed output of the ETL" in {
      val t1 = spark.createDataFrame(BaseCase.Rows1.asJava, BaseCase.Table1)
      val t2 = spark.createDataFrame(BaseCase.Rows2.asJava, BaseCase.Table2)

      BaseCase.etl(t1, t2, BaseCase.Cutoff, BaseCase.Audit).write.insertInto(table)

      spark.table(table).count() shouldBe 2
    }

    "keeps the values through a round trip" in {
      val rows = spark
        .table(table)
        .collect()
        .map(f => f.getAs[String]("OUT_COL_3") -> f)
        .toMap

      rows("C1").getAs[String]("OUT_COL_4") shouldBe "P1"
      rows("C2").getAs[String]("OUT_COL_4") shouldBe "NO_MATCH"
      // The decimal keeps its scale, which is the first thing to get lost.
      rows("C1").getAs[java.math.BigDecimal]("OUT_COL_5") shouldBe new java.math.BigDecimal(
        "300.75"
      )
    }

    "the schema read back is the declared one, not an inferred one" in {
      val types = spark.table(table).schema.fields.map(f => f.name -> f.dataType.simpleString).toMap

      types("OUT_COL_5") shouldBe "decimal(18,2)"
      types("OUT_COL_6") shouldBe "timestamp"
      types("OUT_COL_7") shouldBe "date"
    }

    "append mode accumulates instead of replacing" in {
      val before = spark.table(table).count()
      val t1 = spark.createDataFrame(BaseCase.Rows1.asJava, BaseCase.Table1)
      val t2 = spark.createDataFrame(BaseCase.Rows2.asJava, BaseCase.Table2)

      BaseCase.etl(t1, t2, BaseCase.Cutoff, BaseCase.Audit).write.mode("append").insertInto(table)

      spark.table(table).count() shouldBe before * 2
    }
  }
}
