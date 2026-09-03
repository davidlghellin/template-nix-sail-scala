package devel0pez.etl

import java.nio.file.{Files, Paths}

import scala.io.Source
import scala.util.Using

import org.apache.spark.sql.types.StructType

import devel0pez.Conform

/** Where the data lives and what shape it has — except the shape is a type, not a `StructType`.
  *
  * This is the catalogue entry of the Kedro-style methodology, and the one place where translating
  * it to Scala changes the idea rather than the spelling. In the Python version a dataset carries
  * an explicit `StructType`, and a job's contract with the next job is that two `StructType`s
  * happen to describe the same columns. Here the contract is `T`, and `Conform[T]` derives the
  * schema from it, so there is nothing to keep in sync: the schema cannot drift from the case class
  * because it is generated from it.
  *
  * That moves a whole class of check from run time to compile time. The Python dry-run has to
  * verify that the schema a job produces matches the one the next job consumes; here a `Job[Ciudad,
  * PoblacionCcaa]` chained onto a `Job[Raw, Ciudad]` either typechecks or does not compile. There
  * is therefore no dry-run in this codebase at all, which is the clearest measure of what the types
  * bought.
  *
  * What a type still cannot know is checked where it is used rather than in a phase of its own: a
  * missing path and a CSV header in the wrong order in `Io.read`, two jobs claiming the same output
  * and a cycle in `Graph`, and the chain as a whole in `EtlChainSpec` — running it *is* the
  * dry-run.
  *
  * On the name: the obvious one is `Dataset`, which is what Kedro calls it. It is not used here
  * because `org.apache.spark.sql.Dataset` is in scope in every file that would touch this, and two
  * types called `Dataset` a line apart is a trap rather than a convenience.
  */
final case class DataRef[T](
    name: String,
    path: String,
    format: Format = Format.Csv
)(implicit val shape: Conform[T]) {

  /** The schema, derived from `T` rather than declared beside it. */
  def schema: StructType = shape.schema

  /** This reference's concrete location in a given environment.
    *
    * Paths are declared **relative** and the environment supplies the root, so the same job writes
    * to `./data/...` on a laptop and to `s3://bucket/...` in production without being edited.
    */
  def resolve(config: EtlConfig): String = config.resolve(path)
}

/** How a `DataRef` is stored. Parquet carries its own schema; CSV does not, which is the entire
  * reason the header check below exists.
  */
sealed trait Format extends Product with Serializable

object Format {
  case object Csv extends Format
  case object Parquet extends Format
}

/** Environment and root path, from the environment rather than from a config file.
  *
  * Outside `dev` the root is **required**: without it the job would start and write somewhere
  * plausible and wrong, which is worse than not starting.
  */
final case class EtlConfig(env: String = "dev", dataRoot: String = ".") {

  /** Prepend the root, unless the path escapes it — an absolute path or a URI is left alone, which
    * is how a fixed external source is spelled.
    */
  def resolve(path: String): String =
    if (path.startsWith("/") || path.contains(EtlConfig.UriSeparator)) path
    else if (dataRoot == ".") path
    else s"${dataRoot.stripSuffix("/")}/$path"
}

object EtlConfig {

  val UriSeparator = "://"
  private val GlobChars = Seq("*", "?", "[")

  /** Raised when the environment says production and does not say where. */
  final class MissingRoot(message: String) extends IllegalStateException(message)

  def fromEnv(env: Map[String, String] = sys.env): EtlConfig = {
    val name = env.getOrElse("ETL_ENV", "dev")
    val root = env.get("ETL_DATA_ROOT")
    if (name != "dev" && root.isEmpty) {
      throw new MissingRoot(
        s"ETL_ENV=$name needs ETL_DATA_ROOT: refusing to start rather than write to a relative " +
          "path that happens to exist"
      )
    }
    EtlConfig(name, root.getOrElse("."))
  }

  /** Whether a path is one the local filesystem can be asked about at all.
    *
    * A URI or a glob is the engine's business; `Files.exists` would say "no" and turn a working job
    * into a false alarm.
    */
  def isLocal(path: String): Boolean =
    !path.contains(UriSeparator) && !GlobChars.exists(path.contains)

  /** The header columns of a CSV, read without an engine.
    *
    * A Spark output is a directory of `part-*.csv` sharing one header, so the first will do. `None`
    * means "cannot be checked from here" — a URI, a glob, a missing path — and the check is then
    * left to the read itself.
    */
  def csvHeader(path: String): Option[Seq[String]] = {
    if (!isLocal(path)) return None
    val start = Paths.get(path)
    val file =
      if (Files.isDirectory(start)) {
        Using(Files.list(start))(
          _.toArray.map(_.toString).sorted.find(_.endsWith(".csv"))
        ).toOption.flatten
          .map(Paths.get(_))
      } else if (Files.isRegularFile(start)) Some(start)
      else None

    file.flatMap { f =>
      Using(Source.fromFile(f.toFile, "UTF-8"))(_.getLines().nextOption()).toOption.flatten
        .map(_.split(",", -1).map(_.trim).toSeq)
    }
  }

  /** What is wrong with a header, or `None` if it satisfies the schema.
    *
    * Both halves matter, and the second is the one people are surprised by: an explicit schema is
    * applied **by position**. A file with the right column names in the wrong order is read without
    * complaint and every value ends up under the wrong heading — the same positional trap `Conform`
    * exists for on the write side, arriving here from the read side.
    */
  def headerProblem(schema: StructType, header: Seq[String]): Option[String] = {
    val declared = schema.fieldNames.toSeq
    val missing = declared.filterNot(header.contains)
    if (missing.nonEmpty) {
      Some(
        s"the file is missing declared columns ${missing.mkString("[", ", ", "]")}; " +
          s"it has ${header.mkString("[", ", ", "]")}"
      )
    } else if (header.take(declared.size) != declared) {
      Some(
        s"the columns are in a different order: the schema says ${declared.mkString("[", ", ", "]")} " +
          s"and the file ${header.mkString("[", ", ", "]")}. An explicit schema is applied by " +
          "position, so the values would come back under the wrong names"
      )
    } else None
  }
}
