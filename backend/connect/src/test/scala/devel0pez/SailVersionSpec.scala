package devel0pez

/** The Sail server and the Spark client have to stay paired.
  *
  * Sail picks its Spark configuration from the **client** version, so bumping Spark in
  * versions.json without bumping pysail (or the other way round) pulls them apart. It fails
  * silently: the session connects all the same and the symptoms turn up later, in some unrelated
  * expression.
  */
final class SailVersionSpec extends SparkSuite {

  "the Sail server" - {

    "reports the Spark version declared in versions.json" in {
      // `versions.json` is the single source of truth: build.sbt takes the
      // client version from it and flake.nix takes pysail's.
      val declared = Versions.spark

      spark.version shouldBe declared
    }
  }
}
