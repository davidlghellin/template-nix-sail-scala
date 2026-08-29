package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset, SparkSession}
import org.apache.spark.sql.functions.{coalesce, count, lit, sum, to_date, trim, when}
import org.apache.spark.sql.types.{DecimalType, TimestampType}

import Storage._

/** The one to copy.
  *
  * The other ETLs in this template each isolate a single idea so it can be read on its own —
  * `BaseCase` the DataFrame style, `TypedEtl` the typed one, `PipelineEtl` composable stages,
  * `ConformedEtl` guarded boundaries. This file is what they add up to, on a domain of its own so
  * that it can be lifted whole: meter readings landing as text, coming out as daily usage.
  *
  * Everything it does and why, because the reason is never obvious from the code.
  *
  * **Its own case classes, at file level.** Nested ones drag an outer reference the encoder cannot
  * resolve, and the error `ScalaReflectionException: <none> is not a term` points nowhere near the
  * cause.
  *
  * **Column handles, not strings.** `reading.kwh`, never `col("kwh")`. A typo becomes a compile
  * error instead of an `AnalysisException` twenty minutes in.
  *
  * **`try_cast`, not `cast`.** ANSI is on, so a plain cast over one malformed row raises and takes
  * the whole run with it. Tolerating it as NULL is what gives validation something to route to the
  * dead-letter branch instead.
  *
  * **A type per stage.** `RawReading` to `ParsedReading` to `Reading` to `DailyUsage`. Put two
  * stages in the wrong order and it does not compile.
  *
  * **Stages as values.** `Dataset[A] => Dataset[B]`, so they compose with `andThen` and each one is
  * testable against a one-row `Dataset` built in a test.
  *
  * **`select`, never `withColumn`.** See `DataFrames.addColumns` for the measurement.
  *
  * **`Option` only where absence is legitimate.** `tariff` is optional because a meter can
  * genuinely not have one; `kwh` is not, and what makes it non-null is `validate`, not its type.
  * Careful with the intuition: the schema marks every reference field nullable whether or not it is
  * an `Option`, while a primitive like `readings: Long` comes out non-nullable. `Option` buys an
  * obligation on the Scala side, not a constraint on the data.
  *
  * **Conform at the edges, injected storage.** The job names no table and no format, and cannot
  * skip conforming, because the only door in and out runs it.
  */
/** A reading exactly as it lands: text, because that is what a landing zone holds. */
final case class RawReading(meterId: String, takenAt: String, kwh: String, tariff: String)

object RawReading {
  implicit final class Cols(private val ds: Dataset[RawReading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def tariff: Column = ds("tariff")
  }
}

/** Typed, and still untrustworthy: `try_cast` leaves NULL wherever it could not parse. */
final case class ParsedReading(
    meterId: String,
    takenAt: Timestamp,
    kwh: BigDecimal,
    tariff: Option[String]
)

object ParsedReading {
  implicit final class Cols(private val ds: Dataset[ParsedReading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def tariff: Column = ds("tariff")
  }
}

/** A reading that passed validation.
  *
  * A distinct type from `ParsedReading` with identical fields, and that is the point: `daily`
  * accepts only this one, so aggregating unchecked readings is a compile error. `tariff` stays
  * `Option` because a missing tariff is a fact about the meter, not a defect in the row.
  */
final case class Reading(
    meterId: String,
    takenAt: Timestamp,
    kwh: BigDecimal,
    tariff: Option[String]
)

object Reading {
  implicit final class Cols(private val ds: Dataset[Reading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def tariff: Column = ds("tariff")
  }
}

/** The dead-letter row: the text that failed, and which check failed it. */
final case class RejectedReading(meterId: String, takenAt: String, kwh: String, reason: String)

object RejectedReading {
  implicit final class Cols(private val ds: Dataset[RejectedReading]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def takenAt: Column = ds("takenAt")
    def kwh: Column = ds("kwh")
    def reason: Column = ds("reason")
  }
}

/** What the job produces: one row per meter, day and tariff. */
final case class DailyUsage(
    meterId: String,
    day: Date,
    tariff: String,
    kwh: BigDecimal,
    readings: Long
)

object DailyUsage {
  implicit final class Cols(private val ds: Dataset[DailyUsage]) extends AnyVal {
    def meterId: Column = ds("meterId")
    def day: Column = ds("day")
    def tariff: Column = ds("tariff")
    def kwh: Column = ds("kwh")
    def readings: Column = ds("readings")
  }
}

object MeterEtl {

  /** Text to types. Tolerant on purpose — see point 3. */
  val parse: Dataset[RawReading] => Dataset[ParsedReading] = raw => {
    val spark = raw.sparkSession
    import spark.implicits._
    raw
      .select(
        trim(raw.meterId).as("meterId"),
        raw.takenAt.try_cast(TimestampType).as("takenAt"),
        raw.kwh.try_cast(DecimalType(18, 3)).as("kwh"),
        // An empty tariff and an absent one are the same thing; say so once, here,
        // rather than in every downstream comparison.
        when(trim(raw.tariff) === lit(""), lit(null).cast("string"))
          .otherwise(trim(raw.tariff))
          .as("tariff")
      )
      .as[ParsedReading]
  }

  /** What a row must have to be worth aggregating.
    *
    * One definition, used below as itself and as its negation, so the two branches cannot drift
    * apart and leave a row in both or in neither. `tariff` is absent from it deliberately: it is
    * allowed to be missing.
    */
  private def passes(ds: Dataset[ParsedReading]): Column =
    ds.meterId =!= lit("") && ds.takenAt.isNotNull && ds.kwh.isNotNull && ds.kwh >= lit(0)

  /** The rows that passed, retyped to say so. */
  val validate: Dataset[ParsedReading] => Dataset[Reading] = parsed => {
    val spark = parsed.sparkSession
    import spark.implicits._
    parsed
      .filter(passes(parsed))
      .select(parsed.meterId, parsed.takenAt, parsed.kwh, parsed.tariff)
      .as[Reading]
  }

  /** The rows that did not, with the check that failed them.
    *
    * A second function over the same input rather than one stage returning a pair: a stage has to
    * be `Dataset[A] => Dataset[B]` to compose with `andThen`, and a tuple is not that.
    */
  val rejected: Dataset[ParsedReading] => Dataset[RejectedReading] = parsed => {
    val spark = parsed.sparkSession
    import spark.implicits._
    val reason =
      when(parsed.meterId === lit(""), lit("meter id is empty"))
        .when(parsed.takenAt.isNull, lit("takenAt is not a timestamp"))
        .when(parsed.kwh.isNull, lit("kwh is not a number"))
        .otherwise(lit("kwh is negative"))

    parsed
      .filter(!passes(parsed))
      .select(
        parsed.meterId,
        // Back to text: what failed to parse is exactly what you want to look at.
        parsed.takenAt.cast("string").as("takenAt"),
        parsed.kwh.cast("string").as("kwh"),
        reason.as("reason")
      )
      .as[RejectedReading]
  }

  /** One row per meter, day and tariff.
    *
    * The audit-style literals ride inside the `agg` rather than in a `withColumn` afterwards: a
    * literal is foldable, so Catalyst carries it through the grouping, and the result stays one
    * projection deep. This is also where `Option` ends — a missing tariff becomes `UNKNOWN`,
    * because a grouping key cannot be absent.
    */
  val daily: Dataset[Reading] => Dataset[DailyUsage] = readings => {
    val spark = readings.sparkSession
    import spark.implicits._
    readings
      .groupBy(
        readings.meterId,
        to_date(readings.takenAt).as("day"),
        coalesce(readings.tariff, lit("UNKNOWN")).as("tariff")
      )
      .agg(
        sum(readings.kwh).cast(DecimalType(18, 3)).as("kwh"),
        count(lit(1)).as("readings")
      )
      .as[DailyUsage]
  }

  /** The happy path as one value: landing text to daily usage. */
  val pipeline: Dataset[RawReading] => Dataset[DailyUsage] = parse andThen validate andThen daily

  /** The whole job, with nowhere hardcoded.
    *
    * Three `Storage` instances say where the input, the output and the dead letters live; the job
    * itself names none of them, and cannot write anything without it having been conformed, since
    * `saveTo` is the only door. Swap an instance and it runs somewhere else, unedited.
    *
    * The fork is why this is not simply `pipeline andThen saveTo`: both branches start from the
    * parsed rows, so the shape is a `val` read twice rather than a single chain.
    *
    * Read twice is exactly what it does — parsing runs once per branch. Persisting `parsed` would
    * trade memory for that second pass, and on a real volume it is usually the right trade. It is
    * left out here because a template should show the shape rather than a tuning decision that
    * depends on data you do not have.
    */
  def run(spark: SparkSession)(implicit
      source: Storage[RawReading],
      sink: Storage[DailyUsage],
      deadLetters: Storage[RejectedReading]
  ): Unit = {
    val parsed = spark.load[RawReading].transform(parse)
    parsed.transform(rejected).saveTo
    parsed.transform(validate).transform(daily).saveTo
  }
}
