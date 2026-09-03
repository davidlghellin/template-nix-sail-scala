package devel0pez

import java.sql.Date

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{lit, sum, to_date, when}
import org.apache.spark.sql.types.DecimalType

import Conform._

/** Two sources, joined mid-chain — how composition survives an operation with two inputs.
  *
  * `PipelineEtl` composes a straight line: one dataset in, one out, `andThen` all the way down. A
  * join does not fit that shape, because it takes two. This is what to do about it.
  *
  * The trick is that the two branches are not symmetric in the composition even though they are in
  * the join. The right-hand branch is composed and **finished first**, and the resulting dataset is
  * curried into the joining stage — which leaves that stage as `Dataset[A] => Dataset[B]` like
  * every other, so the main line still reads as one chain:
  *
  * {{{
  * val prices  = readTariffs(spark, tariffTable).transform(parseTariff)   // right branch, done
  * val billing = MeterEtl.parse andThen pricedWith(prices) andThen validate andThen bill
  * readings.transform(billing)
  * }}}
  *
  * Drawn out, the pipeline is a diamond rather than a line, and only the left edge is the one
  * `andThen` walks:
  *
  * {{{
  * readings_raw ──parse──┐
  *                        ├─ join ─→ validate ─→ bill ─→ saveTo
  * tariffs_raw ──parse───┘
  * }}}
  *
  * The order here is join **before** validate, which is deliberate and not the only option. A row
  * is only checkable once both halves are present: "this reading has a price" is not a question the
  * reading alone can answer. Validating each side first and joining after is equally defensible —
  * it just answers different questions, and cannot answer this one.
  *
  * The readings branch is `MeterEtl`'s, reused unchanged. That reuse is the argument for stages
  * being values, made across files rather than inside one.
  */
/** The tariff catalogue as it lands: text, like everything else in a landing zone. */
final case class RawTariff(code: String, pricePerKwh: String)

object RawTariff {
  implicit final class Cols(private val ds: Dataset[RawTariff]) extends AnyVal {
    def code: Column = ds("code")
    def pricePerKwh: Column = ds("pricePerKwh")
  }
}

/** A tariff with a number in it, which it may still not have. */
final case class ParsedTariff(code: String, pricePerKwh: Option[BigDecimal])

object ParsedTariff {
  implicit final class Cols(private val ds: Dataset[ParsedTariff]) extends AnyVal {
    def code: Column = ds("code")
    def pricePerKwh: Column = ds("pricePerKwh")
  }
}

/** A reading with its price alongside — the join's output, before anything is checked. */
final case class PricedReading(
    meterId: String,
    takenAt: java.sql.Timestamp,
    kwh: BigDecimal,
    tariff: Option[String],
    pricePerKwh: Option[BigDecimal]
)

object PricedReading {
  implicit final class Cols(private val ds: Dataset[PricedReading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def tariff: Column = ds("tariff")
    def pricePerKwh: Column = ds("pricePerKwh")
  }
}

/** A priced reading that passed validation: both halves present and usable. */
final case class BillableReading(
    meterId: String,
    takenAt: java.sql.Timestamp,
    kwh: BigDecimal,
    tariff: String,
    pricePerKwh: BigDecimal
)

object BillableReading {
  implicit final class Cols(private val ds: Dataset[BillableReading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def tariff: Column = ds("tariff")
    def pricePerKwh: Column = ds("pricePerKwh")
  }
}

/** What the job produces: what each meter owes, per day and tariff. */
final case class Bill(meterId: String, day: Date, tariff: String, kwh: BigDecimal, cost: BigDecimal)

object BillingEtl {

  /** Read either landing table. Both are text; `conformTo` is what makes them the model. */
  def readReadings(spark: SparkSession, table: String): Dataset[RawReading] =
    spark.table(table).conformTo[RawReading]

  def readTariffs(spark: SparkSession, table: String): Dataset[RawTariff] =
    spark.table(table).conformTo[RawTariff]

  /** The right-hand branch, which is a chain of its own — here, one stage long. */
  val parseTariff: Dataset[RawTariff] => Dataset[ParsedTariff] = raw => {
    val spark = raw.sparkSession
    import spark.implicits._
    raw
      .select(
        raw.code,
        raw.pricePerKwh.try_cast(DecimalType(18, 6)).as("pricePerKwh")
      )
      .as[ParsedTariff]
  }

  /** The join, curried so it is still a stage.
    *
    * `tariffs` is the finished right-hand branch, and passing it in is what keeps the signature
    * `Dataset[A] => Dataset[B]`. A stage that took both datasets as arguments would not compose
    * with `andThen`, and the chain would have to be broken open around it.
    *
    * The join is a LEFT one on purpose: a reading whose tariff is unknown must still reach
    * `validate`, which is the thing that decides what to do about it. Dropping it here would hide
    * the problem in a join condition instead of naming it in a rejection reason.
    */
  def pricedWith(
      tariffs: Dataset[ParsedTariff]
  ): Dataset[ParsedReading] => Dataset[PricedReading] = readings => {
    val spark = readings.sparkSession
    import spark.implicits._
    readings
      .join(tariffs, readings.tariff === tariffs.code, "left")
      .select(
        readings.meterId,
        readings.takenAt,
        readings.kwh,
        readings.tariff,
        tariffs.pricePerKwh
      )
      .as[PricedReading]
  }

  /** What a joined row needs before it can be billed. */
  private def passes(ds: Dataset[PricedReading]): Column =
    ds.meterId =!= lit("") &&
      ds.takenAt.isNotNull &&
      ds.kwh.isNotNull &&
      // Only answerable after the join, which is why validation comes second.
      ds.tariff.isNotNull &&
      ds.pricePerKwh.isNotNull

  /** Validation, now that both halves are on the same row. */
  val validate: Dataset[PricedReading] => Dataset[BillableReading] = priced => {
    val spark = priced.sparkSession
    import spark.implicits._
    priced
      .filter(passes(priced))
      .select(
        priced.meterId,
        priced.takenAt,
        priced.kwh,
        priced.tariff,
        priced.pricePerKwh
      )
      .as[BillableReading]
  }

  /** The rows that could not be billed, and which half was missing. */
  val rejected: Dataset[PricedReading] => Dataset[RejectedReading] = priced => {
    val spark = priced.sparkSession
    import spark.implicits._
    val reason =
      when(priced.meterId === lit(""), lit("meter id is empty"))
        .when(priced.takenAt.isNull, lit("takenAt is not a timestamp"))
        .when(priced.kwh.isNull, lit("kwh is not a number"))
        .when(priced.tariff.isNull, lit("no tariff on the reading"))
        .otherwise(lit("tariff is not in the catalogue"))

    priced
      .filter(!passes(priced))
      .select(
        priced.meterId,
        priced.takenAt.cast("string").as("takenAt"),
        priced.kwh.cast("string").as("kwh"),
        reason.as("reason")
      )
      .as[RejectedReading]
  }

  /** The money, per meter, day and tariff. */
  val bill: Dataset[BillableReading] => Dataset[Bill] = billable => {
    val spark = billable.sparkSession
    import spark.implicits._
    billable
      .groupBy(billable.meterId, to_date(billable.takenAt).as("day"), billable.tariff)
      .agg(
        sum(billable.kwh).cast(DecimalType(18, 3)).as("kwh"),
        sum(billable.kwh * billable.pricePerKwh).cast(DecimalType(18, 2)).as("cost")
      )
      .as[Bill]
  }

  /** The whole left edge as one value, once the right branch has been handed over. */
  def pipeline(tariffs: Dataset[ParsedTariff]): Dataset[RawReading] => Dataset[Bill] =
    MeterEtl.parse andThen pricedWith(tariffs) andThen validate andThen bill

  /** Two tables in, one table out, plus the dead letters. */
  def run(
      spark: SparkSession,
      readingsTable: String,
      tariffsTable: String,
      billsTable: String,
      deadLettersTable: String
  ): Unit = {
    // The right branch, composed and finished before the main chain starts.
    val tariffs = readTariffs(spark, tariffsTable).transform(parseTariff)

    val priced = readReadings(spark, readingsTable)
      .transform(MeterEtl.parse)
      .transform(pricedWith(tariffs))

    priced.transform(rejected).conformTo[RejectedReading].write.insertInto(deadLettersTable)
    priced.transform(validate).transform(bill).conformTo[Bill].write.insertInto(billsTable)
  }
}
