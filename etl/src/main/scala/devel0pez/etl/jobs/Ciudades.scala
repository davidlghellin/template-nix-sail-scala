package devel0pez.etl.jobs

import org.apache.spark.sql.{Column, Dataset}

import devel0pez.etl.{DataRef, Job, Quality}

/** A city, exactly as the CSV carries it.
  *
  * This case class is the schema. There is no `StructType` beside it to keep in step, which is the
  * single biggest difference from the Python original: there, input and output each declare a
  * `StructType`, and a field added to one and not the other is a dry-run finding. Here it is one
  * declaration, and `Conform[Ciudad]` derives the rest.
  */
final case class Ciudad(
    ciudad: String,
    habitantes: Long,
    provincia: String,
    comunidad_autonoma: String,
    superficie_km2: Double
)

object Ciudad {

  /** Column handles, so a transformation names a field rather than a string. */
  implicit final class Cols(private val ds: Dataset[Ciudad]) extends AnyVal {
    def ciudad: Column = ds("ciudad")
    def habitantes: Column = ds("habitantes")
    def provincia: Column = ds("provincia")
    def comunidad_autonoma: Column = ds("comunidad_autonoma")
    def superficie_km2: Column = ds("superficie_km2")
  }
}

/** Validate and deduplicate the cities file.
  *
  * The output type is `Ciudad` again, and that is the truthful statement of what this job does: it
  * removes rows, it does not reshape them. In the Python version the same fact is spelled by
  * pointing both datasets at one shared `CIUDADES_ESQUEMA` and trusting nobody edits one of them.
  */
object Ciudades extends Job[Ciudad, Ciudad] {

  val name = "ciudades"

  /** The key. A string because it names a column, not a field: see `Quality.nonNullKey`. */
  val Clave = "ciudad"

  val consumes: DataRef[Ciudad] = DataRef[Ciudad]("ciudades_raw", "resources/ciudades_espana.csv")
  val produces: DataRef[Ciudad] = DataRef[Ciudad]("ciudades_dedup", "data/ciudades_dedup")

  def transform(input: Dataset[Ciudad]): Dataset[Ciudad] =
    input.transform(validar).transform(deduplicar)

  /** The key must be present and non-null. Present is the type's business; non-null is not. */
  def validar(ds: Dataset[Ciudad]): Dataset[Ciudad] = Quality.nonNullKey(ds, Clave)

  def deduplicar(ds: Dataset[Ciudad]): Dataset[Ciudad] = Quality.deduplicateBy(ds, Clave)
}
