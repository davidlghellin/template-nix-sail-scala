package devel0pez

import org.apache.spark.sql.{Dataset, SparkSession}

import Conform._

/** A typeclass for **where a `T` lives**, so the job does not have to know.
  *
  * An ETL written against it names no tables, no formats and no write modes:
  *
  * {{{
  * spark.load[Sale]                      // reads wherever Sale is configured to live
  *   .transform(stages(...))
  *   .saveTo                             // writes wherever SalesByFamily is configured to live
  * }}}
  *
  * Two things fall out of that, and the second is the one that pays.
  *
  * The first is that **conforming stops being optional**. Both directions run `conformTo[T]` inside
  * the instance, so there is no way to obtain a `Dataset[Sale]` through this door without the frame
  * having been checked and reordered, and no way to write one without it matching the shape
  * `insertInto` will assume. The discipline moves out of the call site, where it was something to
  * remember, and into the type, where it is something to satisfy.
  *
  * The second is that the storage becomes swappable without touching the job. The same ETL runs
  * against a catalogue table or an in-memory view depending only on which instance is in scope —
  * `StorageSpec` runs it both ways and compares the results, which is the sort of thing a test
  * suite can do cheaply once the job has stopped hardcoding its own edges.
  *
  * On naming: the syntax is `load` and `saveTo` rather than `read` and `write` because `Dataset`
  * already has a `write` member, and a member always beats an implicit conversion. An extension
  * called `write` would compile, resolve to Spark's, and silently do something else.
  *
  * On Hive: whether the catalogue is backed by a Hive metastore is a property of the **session** —
  * `enableHiveSupport()` on classic, server configuration on Sail — not something a value can
  * carry. `Storage.catalog` therefore says "this lives in a catalogue table" and leaves the
  * catalogue's identity to whoever built the session, which is also why it works unchanged on both
  * engines.
  */
trait Storage[T] {

  /** Read it, conformed to `T`. */
  def load(spark: SparkSession): Dataset[T]

  /** Write it, conformed to `T`. */
  def save(ds: Dataset[T]): Unit
}

object Storage {

  /** Summon the instance in scope. */
  def apply[T](implicit instance: Storage[T]): Storage[T] = instance

  /** A table in the session catalogue — Hive-backed or not, that is the session's business.
    *
    * Writes with `insertInto`, which matches by **position**, which is exactly why the conform on
    * the way out is not optional.
    */
  def catalog[T: Conform](table: String): Storage[T] = new Storage[T] {
    def load(spark: SparkSession): Dataset[T] = spark.table(table).conformTo[T]
    def save(ds: Dataset[T]): Unit = ds.conformTo[T].write.insertInto(table)
  }

  /** A temporary view: same contract, nothing touches the catalogue or the disk.
    *
    * Its point is not performance, it is proof. Swapping this in for `catalog` shows the job
    * genuinely does not know where its data lives — and gives a test a place to put output without
    * creating a table it then has to remember to drop.
    */
  def view[T: Conform](name: String): Storage[T] = new Storage[T] {
    def load(spark: SparkSession): Dataset[T] = spark.table(name).conformTo[T]
    def save(ds: Dataset[T]): Unit = ds.conformTo[T].createOrReplaceTempView(name)
  }

  /** A **partitioned** catalogue table.
    *
    * Same contract as `catalog`, plus the one rule partitioning adds and nothing enforces: a
    * partition column is moved to the **end** of the table's schema whatever order you declared it
    * in. `PartitionedSpec` measures that, and both engines agree on it.
    *
    * That interacts badly with `insertInto`, which matches by position. A model listing `day` first
    * writes the date into whatever column happens to be first — caught by ANSI on classic as an
    * unrelated-looking cast error, and on Sail not caught at all. So the check here is on the
    * **order** of `T`'s fields, made at write time, where it can name the problem:
    *
    * {{{
    * Storage.partitioned[Daily]("daily", Seq("day"))   // Daily must declare `day` last
    * }}}
    *
    * On reprocessing a single day, this deliberately does not set
    * `spark.sql.sources.partitionOverwriteMode` for you. It is a session-wide setting, silently
    * ignored by Sail — which reports `dynamic` and then performs a static overwrite, deleting every
    * partition the job did not write. `PartitionedSpec` pins that. Choosing it is the caller's
    * decision to make knowingly, not a default to inherit from a storage instance.
    */
  def partitioned[T: Conform](table: String, partitionBy: Seq[String]): Storage[T] =
    new Storage[T] {
      private val fields = Conform[T].schema.fieldNames.toSeq

      // The partition columns have to be exactly the trailing fields, in order.
      private def checkOrder(): Unit = {
        val trailing = fields.takeRight(partitionBy.size)
        if (trailing != partitionBy) {
          throw new ConformError(
            s"partition columns must be the last fields of the model, in order: " +
              s"wanted [${partitionBy.mkString(", ")}] at the end of " +
              s"[${fields.mkString(", ")}], found [${trailing.mkString(", ")}]. " +
              s"A partitioned table reports its partition columns last, and `insertInto` " +
              s"matches by position."
          )
        }
      }

      def load(spark: SparkSession): Dataset[T] = spark.table(table).conformTo[T]

      def save(ds: Dataset[T]): Unit = {
        checkOrder()
        ds.conformTo[T].write.insertInto(table)
      }
    }

  /** `spark.load[Sale]`. Needs `import Storage._`. */
  implicit final class LoadOps(private val spark: SparkSession) extends AnyVal {
    def load[T](implicit instance: Storage[T]): Dataset[T] = instance.load(spark)
  }

  /** `result.saveTo`. Needs `import Storage._`. */
  implicit final class SaveOps[T](private val ds: Dataset[T]) extends AnyVal {
    def saveTo(implicit instance: Storage[T]): Unit = instance.save(ds)
  }
}
