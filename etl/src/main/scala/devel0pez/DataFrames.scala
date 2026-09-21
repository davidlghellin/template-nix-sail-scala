package devel0pez

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

/** Standalone transformations over DataFrames.
  *
  * They take and return a `DataFrame` and never create a session, so they chain freely and can be
  * tested against whichever session the caller already has.
  */
object DataFrames {

  /** Sums two columns and appends the result as a new column. */
  def sumColumns(df: DataFrame, col1: String, col2: String, newCol: String): DataFrame =
    df.select(col("*"), (col(col1) + col(col2)).as(newCol))

  /** Adds several derived columns in a single projection.
    *
    * This is the one place in the template that does not use `select`, and it is here to explain
    * why everywhere else does. `withColumn` adds **one** column and wraps the plan in one more
    * `Project` to do it, so called in a loop it nests.
    *
    * Where it nests is worth being exact about, because the obvious guess is wrong. Catalyst's
    * `CollapseProject` flattens the pile before execution: `ClassicPlanSpec` measures five chained
    * `withColumn`s against the equivalent `select` and finds 6 projections against 2 in the
    * **analyzed** plan, and 1 against 1 once **optimized**. The physical plans are identical. So
    * the cost is not a worse query — it is a longer walk to the same query, paid by the analyzer on
    * every operation that follows. Long enough chains are a known way to blow the analyzer's stack,
    * with a trace that says nothing about the loop that caused it.
    *
    * `withColumns` takes the whole map and produces a single projection. Reach for it when the
    * column list is **computed** rather than written out; when you know the columns, `select` says
    * the same thing and pins the output order besides, which matters the moment the result meets an
    * `insertInto`.
    */
  def addColumns(df: DataFrame, columns: Map[String, Column]): DataFrame =
    df.withColumns(columns)
}
