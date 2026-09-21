package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Column, Dataset}

/** The domain modelled as case classes, one per stage of the ETL.
  *
  * Not every case class in the project lives here, and the rule is worth stating because nothing
  * else says it. This file holds the domain the four teaching ETLs share — `BaseCase`, `TypedEtl`,
  * `PipelineEtl`, `ConformedEtl` — so that reading them side by side compares the *style* rather
  * than the vocabulary. `MeterEtl` and `BillingEtl` declare their own in their own files instead,
  * because those two are meant to be lifted out whole rather than read for contrast.
  *
  * Case classes must be declared at file level, not inside the class that uses them: nested ones
  * drag an outer reference that the encoder cannot resolve, and the failure reads as
  * `ScalaReflectionException: <none> is not a term`, which points nowhere near the cause.
  */
/** A row exactly as it lands: every column a String, because a landing zone has no schema worth the
  * name. `PipelineEtl.parse` is what turns it into something with types.
  */
final case class RawSale(
    country: String,
    branch: String,
    product: String,
    amount: String,
    day: String
)

object RawSale {
  implicit final class Cols(private val ds: Dataset[RawSale]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def product: Column = ds("product")
    def amount: Column = ds("amount")
    def day: Column = ds("day")
  }
}

/** A row as it arrives, straight off the source table. */
final case class Sale(
    country: String,
    branch: String,
    product: String,
    amount: BigDecimal,
    day: Timestamp
)

/** Column handles for a `Dataset[Sale]`, so a join reads `sales.product === products.code` instead
  * of `sales("product") === products("code")`. A typo stops being a runtime `AnalysisException` and
  * starts being a compile error.
  *
  * The handles are **bound to the dataset** rather than free `col(...)` references, and that is the
  * point: when both sides of a join carry a column of the same name, only the qualified form tells
  * Catalyst which one is meant.
  *
  * Living in the companion of `Sale` puts them in the implicit scope of `Dataset[Sale]`, so the
  * call site needs no import.
  *
  * One trap, and it is silent: a field whose name collides with a member of `Dataset` — `count`,
  * `schema`, `columns`, `write` — resolves to the method, because an implicit conversion is only
  * tried when the member does not already exist. Name the field otherwise, or reach for it the long
  * way with `ds("...")`.
  */
object Sale {
  implicit final class Cols(private val ds: Dataset[Sale]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def product: Column = ds("product")
    def amount: Column = ds("amount")
    def day: Column = ds("day")
  }
}

/** A `Sale` that has passed validation.
  *
  * Same fields as `Sale`, and that is the entire point: a **different type** for data that has been
  * checked. `PipelineEtl.enrich` accepts only this one, so feeding it unvalidated rows is a compile
  * error rather than a nasty surprise three stages later. The duplication buys a guarantee the
  * compiler can enforce.
  */
final case class ValidSale(
    country: String,
    branch: String,
    product: String,
    amount: BigDecimal,
    day: Timestamp
)

object ValidSale {
  implicit final class Cols(private val ds: Dataset[ValidSale]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def product: Column = ds("product")
    def amount: Column = ds("amount")
    def day: Column = ds("day")
  }
}

/** A row that failed validation, with the check that rejected it.
  *
  * The dead-letter branch. It keeps the raw identifiers rather than the parsed ones, since what
  * failed to parse is exactly what you want to look at.
  */
final case class RejectedSale(country: String, branch: String, product: String, reason: String)

object RejectedSale {
  implicit final class Cols(private val ds: Dataset[RejectedSale]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def product: Column = ds("product")
    def reason: Column = ds("reason")
  }
}

/** The catalogue the sales are enriched with. */
final case class Product(code: String, name: String, family: String)

object Product {
  implicit final class Cols(private val ds: Dataset[Product]) extends AnyVal {
    def code: Column = ds("code")
    def name: Column = ds("name")
    def family: Column = ds("family")
  }
}

/** A sale once it has been joined and normalised, still per row. */
final case class EnrichedSale(
    country: String,
    branch: String,
    product: String,
    family: String,
    amount: BigDecimal
)

object EnrichedSale {
  implicit final class Cols(private val ds: Dataset[EnrichedSale]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def product: Column = ds("product")
    def family: Column = ds("family")
    def amount: Column = ds("amount")
  }
}

/** What ends up in the target table: one row per country, branch and family. */
final case class SalesByFamily(
    country: String,
    branch: String,
    family: String,
    total: BigDecimal,
    audited: Timestamp,
    day: Date
)

object SalesByFamily {
  implicit final class Cols(private val ds: Dataset[SalesByFamily]) extends AnyVal {
    def country: Column = ds("country")
    def branch: Column = ds("branch")
    def family: Column = ds("family")
    def total: Column = ds("total")
    def audited: Column = ds("audited")
    def day: Column = ds("day")
  }
}

/** An event as it lands, with a nested array — the shape that breaks encoders first when a client
  * is strict about types.
  */
final case class Event(
    userId: Long,
    ts: Timestamp,
    eventType: String,
    amount: Double,
    tags: Seq[String]
)

/** The dimension the events are enriched with. */
final case class User(userId: Long, country: String, segment: String)
