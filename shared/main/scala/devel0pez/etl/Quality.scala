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
    val rowId = "__etl_row_id__"
    val rowNumber = "__etl_row_number__"
    val original = ds.columns.map(col)
    val encoder = ds.encoder

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
