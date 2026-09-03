package devel0pez

import java.math.BigDecimal
import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{coalesce, col, lit, sum, when}
import org.apache.spark.sql.types._

/** The compatibility base case: schemas, an ETL, and conforming to a target.
  *
  * It has the shape of a real ETL rather than of an example: two tables with explicit
  * `StructType`s, a filter with `CASE` + `DISTINCT`, a `LEFT JOIN` qualified by DataFrame (both
  * tables carry columns of the same name), an aggregate over `DecimalType`, and a positional
  * conform to the target schema so the result can be handed to `insertInto`.
  *
  * None of the transformations creates a session: they take DataFrames and return DataFrames, so
  * the same code runs against whichever engine the caller connected to.
  */
object BaseCase {

  val Cutoff: Timestamp = Timestamp.valueOf("2024-12-31 00:00:00")
  val Audit: Timestamp = Timestamp.valueOf("2025-01-20 15:04:31")

  val Table1: StructType = StructType(
    Seq(
      StructField("TABLE_1_COL_1", StringType), // key
      StructField("TABLE_1_COL_2", StringType), // key
      StructField("TABLE_1_COL_3", StringType), // key
      StructField("TABLE_1_COL_4", DecimalType(18, 2)), // amount
      StructField("TABLE_1_COL_5", TimestampType) // date
    )
  )

  val Table2: StructType = StructType(
    Seq(
      StructField("TABLE_2_COL_1", StringType), // key
      StructField("TABLE_2_COL_2", StringType), // key, normalised
      StructField("TABLE_2_COL_3", StringType), // key
      StructField("TABLE_2_COL_4", StringType), // attribute to bring over
      StructField("TABLE_2_COL_5", StringType), // type, drives the filter
      StructField("TABLE_2_COL_6", TimestampType) // date
    )
  )

  /** The partition column goes LAST: `insertInto` matches by position, not by name. */
  val TableOut: StructType = StructType(
    Seq(
      StructField("OUT_COL_1", StringType),
      StructField("OUT_COL_2", StringType),
      StructField("OUT_COL_3", StringType),
      StructField("OUT_COL_4", StringType),
      StructField("OUT_COL_5", DecimalType(18, 2)),
      StructField("OUT_COL_6", TimestampType),
      StructField("OUT_COL_7", DateType)
    )
  )

  val Rows1: Seq[Row] = Seq(
    Row("ES", "0182", "C1", new BigDecimal("100.50"), Cutoff),
    Row("ES", "0182", "C1", new BigDecimal("200.25"), Cutoff), // aggregates with the previous one
    Row("ES", "0182", "C2", new BigDecimal("10.00"), Cutoff) // no match -> coalesce
  )

  val Rows2: Seq[Row] = Seq(
    Row("ES", "0227", "C1", "P1", "TIT", Cutoff), // 0227 -> 0182
    Row("ES", "0182", "C1", "P1", "TIT", Cutoff), // duplicate -> distinct
    Row("ES", "0182", "C9", "P9", "AUT", Cutoff) // not TIT -> filtered out
  )

  /** Filter by date and type, normalise with CASE, then DISTINCT. */
  def filterAndDeduplicate(t2: DataFrame, cutoff: Timestamp): DataFrame = {
    val c = lit(cutoff).cast("timestamp")
    t2.filter(col("TABLE_2_COL_6") === c && col("TABLE_2_COL_5") === "TIT")
      .select(
        col("TABLE_2_COL_1"),
        when(col("TABLE_2_COL_2").isin("0227", "0057"), lit("0182"))
          .otherwise(col("TABLE_2_COL_2"))
          .as("TABLE_2_COL_2"),
        col("TABLE_2_COL_3"),
        col("TABLE_2_COL_4")
      )
      .distinct()
  }

  /** LEFT JOIN qualified by DataFrame, then coalesce and groupBy/sum. */
  def joinAndAggregate(t1: DataFrame, t2: DataFrame, audit: Timestamp): DataFrame = {
    val j = t1
      .join(
        t2,
        t2("TABLE_2_COL_1") === t1("TABLE_1_COL_1")
          && t2("TABLE_2_COL_2") === t1("TABLE_1_COL_2")
          && t2("TABLE_2_COL_3") === t1("TABLE_1_COL_3"),
        "left"
      )
      .select(
        t1("TABLE_1_COL_1").as("OUT_COL_1"),
        t1("TABLE_1_COL_2").as("OUT_COL_2"),
        t1("TABLE_1_COL_3").as("OUT_COL_3"),
        coalesce(t2("TABLE_2_COL_4"), lit("NO_MATCH")).as("OUT_COL_4"),
        t1("TABLE_1_COL_4").as("OUT_COL_5"),
        t1("TABLE_1_COL_5").as("OUT_COL_7")
      )
    j.groupBy("OUT_COL_1", "OUT_COL_2", "OUT_COL_3", "OUT_COL_4", "OUT_COL_7")
      .agg(sum("OUT_COL_5").as("OUT_COL_5"))
      .select(col("*"), lit(audit).cast("timestamp").as("OUT_COL_6"))
  }

  /** Orders and casts to the target schema. This is what `INSERT INTO (cols)` would do. */
  def conform(df: DataFrame, schema: StructType): DataFrame = {
    val missing = schema.fields.map(_.name).filterNot(df.columns.contains)
    if (missing.nonEmpty) {
      throw new IllegalArgumentException(s"missing columns: ${missing.mkString(", ")}")
    }
    df.select(schema.fields.toIndexedSeq.map(f => col(f.name).cast(f.dataType).as(f.name)): _*)
  }

  /** The whole ETL: from the two source tables to the output schema. */
  def etl(t1: DataFrame, t2: DataFrame, cutoff: Timestamp, audit: Timestamp): DataFrame =
    conform(joinAndAggregate(t1, filterAndDeduplicate(t2, cutoff), audit), TableOut)
}
