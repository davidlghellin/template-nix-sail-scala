package devel0pez

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.{Dataset, SparkSession}

import Conform._

/** An ETL whose **boundaries** are the whole point.
  *
  * The middle of this one is not new: it is `PipelineEtl`'s three stages, reused unchanged. That
  * reuse is itself the argument for stages being values — a new job can adopt the whole
  * transformation with an `andThen` and spend its own code on what actually differs.
  *
  * What differs is the two edges, which is where ETLs really break:
  *
  *   - **Reading.** A source table belongs to somebody else. Its columns are in whatever order that
  *     team found convenient, and there are more of them than the model wants. `as[Sale]` accepts
  *     that quietly and hands back a `Dataset[Sale]` whose schema is still the table's — wrong
  *     order, extra columns and all. `conformTo[Sale]` makes the frame match the model, and fails
  *     on the spot if a field it needs is not there.
  *   - **Writing.** `insertInto` matches by **position**. Producing the right columns in the right
  *     order by hand is discipline, and discipline is what stops working the day somebody adds a
  *     column to an `agg`. `conformTo[SalesByFamily]` turns it into a guarantee.
  *
  * The failure this prevents does not look like a failure: it looks like a table with the family
  * written into the code column and a job that exited zero. `ConformSpec` asserts that damage
  * directly; this is the same lesson applied to a real end-to-end run.
  *
  * The contract that comes with it: the case class **is** the schema. `conformTo[SalesByFamily]`
  * orders the columns as `SalesByFamily` declares them, so the target table has to be declared in
  * that same order. That is a fair trade — one ordering, written once in Scala, instead of an
  * ordering implied by every `select` along the way.
  */
object ConformedEtl {

  /** The read boundary: whatever shape the source is in, this is a `Dataset[Sale]` or an error. */
  def read(spark: SparkSession, table: String): Dataset[Sale] =
    spark.table(table).conformTo[Sale]

  /** The middle, borrowed wholesale from `PipelineEtl`. */
  def stages(
      products: Dataset[Product],
      audited: Timestamp,
      day: Date
  ): Dataset[Sale] => Dataset[SalesByFamily] =
    PipelineEtl.validate andThen
      PipelineEtl.enrich(products) andThen
      PipelineEtl.byFamily(audited, day)

  /** The write boundary: conform, then insert. Never insert without conforming. */
  def write(result: Dataset[SalesByFamily], table: String): Unit =
    result.conformTo[SalesByFamily].write.insertInto(table)

  /** The same ETL with its edges **injected** rather than passed.
    *
    * Nothing here names a table, a format or a write mode: `Storage[Sale]` says where the input
    * lives, `Storage[SalesByFamily]` where the output goes, and both conform on the way through.
    * Swap either instance and the job runs somewhere else without being edited — which is the
    * difference between an ETL that is configurable and one that merely takes arguments.
    */
  def runStored(
      spark: SparkSession,
      products: Dataset[Product],
      audited: Timestamp,
      day: Date
  )(implicit source: Storage[Sale], sink: Storage[SalesByFamily]): Unit = {
    import Storage._
    spark.load[Sale].transform(stages(products, audited, day)).saveTo
  }

  /** Source table to target table, both edges guarded, names passed in. */
  def run(
      spark: SparkSession,
      source: String,
      products: Dataset[Product],
      target: String,
      audited: Timestamp,
      day: Date
  ): Unit =
    write(read(spark, source).transform(stages(products, audited, day)), target)
}
