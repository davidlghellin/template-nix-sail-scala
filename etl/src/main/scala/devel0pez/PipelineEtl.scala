package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, lit, sum, trim, when}
import org.apache.spark.sql.types.{DateType, DecimalType, TimestampType}

/** The same ETL as `TypedEtl`, but built out of stages that compose.
  *
  * `TypedEtl` writes each step as a method that takes a `Dataset` and returns one. This one writes
  * each step as a **value** of type `Dataset[A] => Dataset[B]`, which changes what you can do with
  * it: a method can only be called, a function can also be composed, named, passed around and
  * tested on its own.
  *
  * Two operations carry the idea, and they are related without being the same thing:
  *
  *   - `ds.transform(f)` is `f(ds)`. It **applies**, and exists only so a chain reads left to right
  *     instead of nesting as `byFamily(enrich(validate(parse(ds))))`.
  *   - `f andThen g` is `x => g(f(x))`. It **composes**, and hands back another function; no
  *     `Dataset` has been touched yet.
  *
  * They meet at `ds.transform(f).transform(g) == ds.transform(f andThen g)`, which is what makes
  * `pipeline` below the whole ETL as a single value. `PipelineEtlSpec` asserts that equality rather
  * than leaving it as a claim.
  *
  * `transform` is also safe over Spark Connect, and for a reason worth knowing: it is a concrete
  * method on the shared API, so the function runs on the **client** while the plan is being built.
  * Nothing is shipped. That is the opposite of `map`, whose closure would have to be executed on a
  * server that has no JVM — see `TypedEtl` for that boundary.
  *
  * The stage types are the other half of the design. `RawSale -> Sale -> ValidSale -> EnrichedSale
  * -> SalesByFamily` is not decoration: put two stages in the wrong order and the compile fails.
  * With a `DataFrame` at every step the same mistake is an `AnalysisException` ten minutes into the
  * job.
  */
object PipelineEtl {

  /** Reads the landing table, where every column is a String. */
  def read(spark: SparkSession, table: String): Dataset[RawSale] = {
    import spark.implicits._
    spark.table(table).as[RawSale]
  }

  /** Stage 1 — give the strings types.
    *
    * `try_cast`, not `cast`, and that is the decision the rest of the pipeline is shaped around.
    * The suite runs with ANSI mode on, so a plain `cast` over a malformed value raises and takes
    * the whole job with it — one bad row in a million kills the run. `try_cast` yields NULL
    * instead, which turns a fatal error into a row that `validate` can route to the dead-letter
    * branch.
    */
  val parse: Dataset[RawSale] => Dataset[Sale] = raw => {
    val spark = raw.sparkSession
    import spark.implicits._
    raw
      .select(
        trim(raw.country).as("country"),
        trim(raw.branch).as("branch"),
        trim(raw.product).as("product"),
        raw.amount.try_cast(DecimalType(18, 2)).as("amount"),
        raw.day.try_cast(TimestampType).as("day")
      )
      .as[Sale]
  }

  /** What a parsed row has to satisfy to be worth enriching.
    *
    * One definition, used by both branches below as itself and as its negation, so the two cannot
    * drift apart and leave rows belonging to neither or to both.
    */
  private def passes(sale: Dataset[Sale]): Column =
    sale.amount.isNotNull &&
      sale.day.isNotNull &&
      sale.country =!= lit("") &&
      sale.branch =!= lit("") &&
      sale.product =!= lit("")

  /** Stage 2 — keep the rows that passed, and change their type to say so. */
  val validate: Dataset[Sale] => Dataset[ValidSale] = sale => {
    val spark = sale.sparkSession
    import spark.implicits._
    sale
      .filter(passes(sale))
      .select(sale.country, sale.branch, sale.product, sale.amount, sale.day)
      .as[ValidSale]
  }

  /** The other branch: the rows that did not pass, with the reason they did not.
    *
    * Deliberately a second function over the same input rather than one stage returning a pair. A
    * stage has to be `Dataset[A] => Dataset[B]` to compose with `andThen`, and a tuple is not that.
    * Splitting keeps the happy path composable and the dead letters a separate write.
    */
  val rejected: Dataset[Sale] => Dataset[RejectedSale] = sale => {
    val spark = sale.sparkSession
    import spark.implicits._
    val reason =
      when(sale.amount.isNull, lit("amount is not a number"))
        .when(sale.day.isNull, lit("day is not a timestamp"))
        .otherwise(lit("a key field is empty"))

    sale
      .filter(!passes(sale))
      .select(sale.country, sale.branch, sale.product, reason.as("reason"))
      .as[RejectedSale]
  }

  /** Stage 3 — join the catalogue in.
    *
    * Takes `products` and gives back a stage. The catalogue is configuration of the stage, not
    * input to the pipeline, and currying it is what keeps the signature `Dataset[A] => Dataset[B]`
    * so it still composes.
    */
  def enrich(products: Dataset[Product]): Dataset[ValidSale] => Dataset[EnrichedSale] = valid => {
    val spark = valid.sparkSession
    import spark.implicits._
    valid
      .join(products, valid.product === products.code, "left")
      .select(
        valid.country,
        valid.branch,
        valid.product,
        // A sale whose product is not in the catalogue still has to come out.
        coalesce(products.family, lit("UNKNOWN")).as("family"),
        valid.amount
      )
      .as[EnrichedSale]
  }

  /** Stage 4 — aggregate, and stamp the audit columns inside the `agg`. */
  def byFamily(
      audited: Timestamp,
      day: Date
  ): Dataset[EnrichedSale] => Dataset[SalesByFamily] = enriched => {
    val spark = enriched.sparkSession
    import spark.implicits._
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

  /** The four stages as one function, landing table to target table.
    *
    * This is what the shape buys. The ETL now has a type — `Dataset[RawSale] =>
    * Dataset[SalesByFamily]` — and is a value like any other: it can be held, handed to something
    * else, or applied with `raw.transform(pipeline(...))`.
    */
  def pipeline(
      products: Dataset[Product],
      audited: Timestamp,
      day: Date
  ): Dataset[RawSale] => Dataset[SalesByFamily] =
    parse andThen validate andThen enrich(products) andThen byFamily(audited, day)
}
