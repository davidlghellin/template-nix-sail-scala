package devel0pez.etl

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, monotonically_increasing_id, row_number}

/** Quality checks over a typed `Dataset`, and what is left of them once the type is doing its job.
  *
  * The Python version's `check_required_columns` is the one that disappears. It exists there
  * because a `DataFrame` can be missing a column and nobody notices until something reads it; here
  * a `Dataset[Ciudad]` came through `Conform[Ciudad]`, which already refused anything that did not
  * have the fields. Reimplementing it would be asserting the type system works.
  *
  * What does not disappear is anything about **values**. A key can be present and null; rows can be
  * duplicated. No case class expresses either, so both stay.
  */
object Quality {

  /** A column name derived from `taken`, so it cannot be one of them. */
  private def freshName(base: String, taken: Set[String]): String = {
    val start = s"__etl_${base}__"
    if (!taken.contains(start)) start
    else Iterator.from(1).map(i => s"__etl_${base}_${i}__").find(!taken.contains(_)).get
  }

  /** Fail when the key has nulls, naming how many.
    *
    * The column is named rather than reached for by field, because a nullable field on a case class
    * is a `String` either way — the type says a value may be absent, and this is the check that
    * decides it may not be.
    */
  def nonNullKey[T](ds: Dataset[T], keyCol: String): Dataset[T] = {
    val nulls = ds.filter(col(keyCol).isNull).count()
    if (nulls > 0) throw new QualityError(s"key '$keyCol' has $nulls null values")
    ds
  }

  /** One row per key, keeping the first occurrence in read order.
    *
    * `monotonically_increasing_id` is increasing within a partition, so on a file read in one pass
    * it reproduces the source order — but it is not a total order if the source was repartitioned.
    * When the choice actually matters, sort by a business column first.
    *
    * Written with a window rather than `dropDuplicates` on purpose: `dropDuplicates` picks an
    * arbitrary row, and "arbitrary" is the kind of thing that is stable in tests and not in
    * production.
    */
  def deduplicateBy[T](ds: Dataset[T], keyCol: String): Dataset[T] = {
    val original = ds.columns.map(col)
    val encoder = ds.encoder

    // The two scratch columns are named against the frame rather than fixed,
    // and the reason is a silent corruption rather than a clash. `withColumn`
    // on a name that already exists **replaces** it, and the `select` below
    // then hands back the replacement — so a dataset that happened to carry a
    // field called `__etl_row_id__` came out with its values overwritten by the
    // monotonic ids, no error anywhere. Deriving the name from the columns
    // present makes that unreachable instead of unlikely.
    val rowId = freshName("row_id", ds.columns.toSet)
    val rowNumber = freshName("row_number", ds.columns.toSet + rowId)

    ds.withColumn(rowId, monotonically_increasing_id())
      .withColumn(
        rowNumber,
        row_number().over(Window.partitionBy(col(keyCol)).orderBy(col(rowId).asc))
      )
      .filter(col(rowNumber) === 1)
      .select(original: _*)
      .as(encoder)
  }
}
