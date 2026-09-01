package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, sum}
import org.apache.spark.sql.types.{DateType, DecimalType, TimestampType}

/** The same ETL as `BaseCase`, but typed with case classes end to end.
  *
  * Every step is expressed with **columns**, never with a typed lambda, and that is not a style
  * choice. Spark Connect ships the plan to the server, and a lambda is JVM bytecode: a Rust engine
  * has nothing to run it with, so `map(x => ...)`, `filter(_.field > 0)` and `groupByKey` come back
  * as `Scala UDF is not supported yet`. Encoders are a client-side matter, so `as[T]` and
  * `Dataset[T]` work perfectly well on both engines.
  *
  * The restriction is not a workaround for Sail either: a typed `map` has always been opaque to
  * Catalyst, so the column form is the one you wanted anyway.
  *
  * What the columns are not is stringly typed. Every reference below goes through the handles in
  * `Model` — `sales.product`, `products.code` — so a renamed field breaks the compile instead of
  * surviving to an `AnalysisException` at run time. Strings appear only where a column is being
  * *named* rather than read: `as("family")`, `as("total")`. Casts go through the type objects
  * (`DecimalType(18, 2)`) rather than their string spellings, for the same reason.
  *
  * There is no `withColumn` here either, and that is deliberate — see `DataFrames.addColumns` for
  * why, and for the one case that earns the exception.
  */
object TypedEtl {

  /** Reads the source table as a `Dataset[Sale]` instead of a `DataFrame`. */
  def sales(spark: SparkSession, table: String): Dataset[Sale] = {
    import spark.implicits._
    spark.table(table).as[Sale]
  }

  /** Joins sales with the catalogue, keeping the row count. */
  def enrich(sales: Dataset[Sale], products: Dataset[Product]): Dataset[EnrichedSale] = {
    val spark = sales.sparkSession
    import spark.implicits._
    sales
      .join(products, sales.product === products.code, "left")
      .select(
        sales.country,
        sales.branch,
        sales.product,
        // A sale whose product is not in the catalogue still has to come out.
        coalesce(products.family, lit("UNKNOWN")).as("family"),
        sales.amount
      )
      .as[EnrichedSale]
  }

  /** Aggregates by country, branch and family, and stamps the audit columns. */
  def byFamily(
      enriched: Dataset[EnrichedSale],
      audited: Timestamp,
      day: Date
  ): Dataset[SalesByFamily] = {
    val spark = enriched.sparkSession
    import spark.implicits._
    // The audit stamps ride inside the `agg` rather than in a `withColumn` afterwards. Two
    // reasons. A literal is foldable, so it needs no aggregation and Catalyst is happy to carry
    // it through the grouping. And once `groupBy(...).agg(...)` has run the result is a plain
    // `DataFrame` whose columns are no longer tied to `EnrichedSale`: anything added later could
    // only name them by string, which is what these handles exist to avoid.
    enriched
      .groupBy(enriched.country, enriched.branch, enriched.family)
      .agg(
        // The sum widens the decimal; the target schema decides the scale.
        sum(enriched.amount).cast(DecimalType(18, 2)).as("total"),
        lit(audited).cast(TimestampType).as("audited"),
        lit(day).cast(DateType).as("day")
      )
      .as[SalesByFamily]
  }
}
