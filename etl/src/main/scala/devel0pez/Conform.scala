package devel0pez

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.spark.sql.types.StructType

/** Raised when a `DataFrame` cannot be shaped into a `Dataset[T]`, naming what is wrong. */
final class ConformError(message: String) extends IllegalArgumentException(message)

/** A typeclass for "this frame can become a `Dataset[T]`": reorder to `T`'s fields, drop what `T`
  * does not declare, and fail loudly when something it does declare is not there.
  *
  * It exists because `as[T]` alone is not enough, and the way it is not enough is silent. `as[T]`
  * **decodes by name but does not reorder the schema**: a frame whose columns are `(family, name,
  * code)` becomes a `Dataset[Product]` whose schema is still in that order, while `collect()` hands
  * back perfectly correct `Product` values. Every assertion you write passes. Then `insertInto`
  * matches by **position** and writes the family into the code column.
  *
  * Spark has the engine for the fix in `Dataset.to(StructType)`, which reorders, drops the extras
  * and type-checks. What it lacks is the guard: a field the frame does not have is filled with
  * `null` rather than refused — and since a schema derived from a case class has every *reference*
  * field `nullable = true`, that path is always open. `ConformSpec` pins both behaviours down.
  *
  * The typeclass is derived from `Encoders.product[T]`, which needs only a `TypeTag`, so an
  * instance can be summoned anywhere — no `SparkSession`, no `import spark.implicits._`.
  *
  * Note the `scala.Product` bound below, spelled out rather than left as `Product`: this package
  * declares a case class of that name for the catalogue, and it would shadow the one meant here.
  */
trait Conform[T] {

  /** The shape `T` demands. */
  def schema: StructType

  /** Shape `df` to it, or throw a `ConformError` saying what is missing. */
  def apply(df: Dataset[_]): Dataset[T]
}

object Conform {

  /** Summon the instance in scope: `Conform[Sale].schema`. */
  def apply[T](implicit instance: Conform[T]): Conform[T] = instance

  /** The everyday instance: fails on a missing field, **projects away** anything extra.
    *
    * Extra columns are not an error because dropping them is the normal case — reading a wide table
    * into a narrow model is a projection you asked for, not a mistake. And the mistake people
    * actually fear, a mistyped column name, already shows up on the other branch: a typo makes the
    * field missing as well as leaving a stray one behind.
    */
  implicit def projecting[T <: scala.Product: TypeTag]: Conform[T] =
    instance(Encoders.product[T], rejectExtra = false)

  /** The contract instance: also fails when the frame carries a column `T` does not declare.
    *
    * Not the default, but the right choice at a boundary — a landing zone, or a target table read
    * back — where a column appearing out of nowhere means something upstream changed and the quiet
    * answer is the wrong one. Pass it explicitly: `df.conformTo[Sale](Conform.exact)`.
    */
  def exact[T <: scala.Product: TypeTag]: Conform[T] =
    instance(Encoders.product[T], rejectExtra = true)

  /** The same schema with every field marked nullable.
    *
    * `StructType.asNullable` does this already but is `private[sql]`, so it is rebuilt here. Nested
    * structs recurse; the element nullability inside arrays and maps is left alone, which is enough
    * for the models in this template and worth knowing if yours are deeper.
    */
  private def relaxed(schema: StructType): StructType =
    StructType(schema.fields.map { field =>
      field.dataType match {
        case nested: StructType => field.copy(dataType = relaxed(nested), nullable = true)
        case _                  => field.copy(nullable = true)
      }
    })

  private def instance[T](encoder: Encoder[T], rejectExtra: Boolean): Conform[T] =
    new Conform[T] {

      val schema: StructType = encoder.schema

      def apply(df: Dataset[_]): Dataset[T] = {
        val wanted = schema.fieldNames.toSeq
        val present = df.columns.toSeq
        val missing = wanted.diff(present)
        val extra = present.diff(wanted)

        // Checked before `to`, not after, because `to` would quietly fill a missing nullable
        // field with nulls and hand back something that looks fine all the way to the table.
        if (missing.nonEmpty) {
          throw new ConformError(
            s"missing columns: ${missing.mkString(", ")}. " +
              s"wanted [${wanted.mkString(", ")}], got [${present.mkString(", ")}]"
          )
        }
        if (rejectExtra && extra.nonEmpty) {
          throw new ConformError(
            s"unexpected columns: ${extra.mkString(", ")}. " +
              s"wanted exactly [${wanted.mkString(", ")}], got [${present.mkString(", ")}]"
          )
        }

        // `asNullable`, and this one is not cosmetic. `to` also enforces
        // nullability, and a case class marks **primitives** non-nullable:
        // `readings: Long` is `nullable = false` while `kwh: BigDecimal` is
        // not, because one is an AnyVal and the other a reference. Every
        // column read back from a table is nullable, so without this,
        // conforming a table into any model with a primitive field fails —
        // on grounds that have nothing to do with shape. Relaxing the target
        // keeps `to` doing what it is here for, and leaves non-nullness where
        // this template says it belongs: in validation, not in a type.
        df.to(relaxed(schema)).as(encoder)
      }
    }

  /** `df.conformTo[Sale]`, the way this is meant to be called. Needs `import Conform._` at the call
    * site: the conversion lives here, and `Dataset`'s implicit scope does not reach it.
    */
  implicit final class ConformOps(private val df: Dataset[_]) extends AnyVal {
    def conformTo[T](implicit instance: Conform[T]): Dataset[T] = instance(df)
  }
}
