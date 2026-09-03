package devel0pez.etl.jobs

import org.apache.spark.sql.{Column, Dataset}
import org.apache.spark.sql.functions.{col, sum}

import devel0pez.etl.{DataRef, Job}

/** Population totalled by autonomous community. A different shape, so a different type. */
final case class PoblacionCcaa(comunidad_autonoma: String, habitantes: Long)

object PoblacionCcaa {
  implicit final class Cols(private val ds: Dataset[PoblacionCcaa]) extends AnyVal {
    def comunidad_autonoma: Column = ds("comunidad_autonoma")
    def habitantes: Column = ds("habitantes")
  }
}

/** Aggregate the deduplicated cities by community.
  *
  * Note what declares the dependency on the previous job: `consumes = Ciudades.produces`. Not a
  * path that happens to match, not a name repeated in two files — the same value, so the link is an
  * ordinary reference the compiler resolves and the IDE navigates. Delete `Ciudades.produces` and
  * this stops compiling, which is the property the Python version buys with an import and a
  * dry-run.
  *
  * The type parameter carries the other half: this is a `Job[Ciudad, PoblacionCcaa]`, so it can
  * only be fed something that produces `Ciudad`.
  */
object PorCcaa extends Job[Ciudad, PoblacionCcaa] {

  val name = "por_ccaa"

  val Clave = "comunidad_autonoma"

  val consumes: DataRef[Ciudad] = Ciudades.produces
  val produces: DataRef[PoblacionCcaa] =
    DataRef[PoblacionCcaa]("poblacion_por_ccaa", "data/poblacion_por_ccaa")

  /** Columns rather than a closure, for the reason the rest of this template measures: a closure
    * does not travel to Sail, and on classic it quietly costs the pushdown.
    */
  def transform(input: Dataset[Ciudad]): Dataset[PoblacionCcaa] = {
    val encoder = org.apache.spark.sql.Encoders.product[PoblacionCcaa]
    input
      .groupBy(col(Clave))
      .agg(sum(col("habitantes")).as("habitantes"))
      .orderBy(col("habitantes").desc)
      .as(encoder)
  }
}
