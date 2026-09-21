package devel0pez

import scala.io.Source
import scala.util.Using

/** Reads versions.json, the same file build.sbt and flake.nix read. */
object Versions {

  private lazy val json: String =
    Using.resource(Source.fromFile("versions.json"))(_.mkString)

  private def read(key: String): String = {
    val re = ("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"").r
    re.findFirstMatchIn(json)
      .map(_.group(1))
      .getOrElse(sys.error(s"versions.json has no key '$key'"))
  }

  lazy val spark: String = read("spark")
  lazy val pysail: String = read("pysail")
  lazy val scala: String = read("scala")
}
