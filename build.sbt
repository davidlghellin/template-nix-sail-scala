ThisBuild / scalaVersion := versionOf("scala")
ThisBuild / version := "0.1.0"
ThisBuild / organization := "devel0pez"

// sbt forks its JVMs (compile and test) from the one that launched it, not
// from JAVA_HOME. Nixpkgs packages sbt with a JDK of its own, which is not the
// devshell's, so the shell has to win: pin it by hand.
ThisBuild / javaHome := sys.env.get("JAVA_HOME").map(file)

// Versions come from versions.json, which flake.nix reads too. Sail derives
// its Spark configuration from the **client** version, so bumping Spark here
// without bumping pysail there would pull them apart; with a single source
// that cannot happen. Scala 2.13 is not a choice: Spark 4 only publishes
// artifacts for 2.13.
def versionOf(key: String): String = {
  val json = IO.read(file("versions.json"))
  val re = ("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"").r
  re.findFirstMatchIn(json).map(_.group(1)).getOrElse {
    sys.error(s"versions.json has no key '$key'")
  }
}

val sparkVersion = versionOf("spark")
val scalaTestVersion = "3.2.19"

// Spark and Arrow reach into JDK internals by reflection, and those have been
// sealed since Java 17. `spark-submit` adds these flags on your behalf; when
// you launch from sbt nobody does, and the first `collect()` dies with
// InaccessibleObjectException (classic) or with "sun.misc.Unsafe not
// available" from Arrow (connect). They are needed at run time, not at
// compile time, which is why they hang off `javaOptions` and not
// `scalacOptions`, and why a build that compiles fine can still fail on its
// first row.
val jvmOptions = Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Djdk.reflect.useDirectMethodHandle=false",
  "-Dio.netty.tryReflectionSetAccessible=true"
)

// The domain code and its tests are **the same** for both backends: they are
// compiled twice, once against each Spark client. This is the equivalent of
// `SPARK_BACKEND=pysail|pyspark` in the Python template, except that there the
// choice is made at run time and here at compile time, because `spark-sql` and
// `spark-connect-client-jvm` cannot share a classpath: both ship
// `org.apache.spark.sql.SparkSession`.
lazy val common = Seq(
  Compile / unmanagedSourceDirectories += (ThisBuild / baseDirectory).value / "shared" / "main" / "scala",
  Test / unmanagedSourceDirectories += (ThisBuild / baseDirectory).value / "shared" / "test" / "scala",
  // Shared test logging config, so a green run stays quiet on both backends.
  Test / unmanagedResourceDirectories += (ThisBuild / baseDirectory).value / "shared" / "test" / "resources",
  libraryDependencies += "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  // One SparkSession per JVM: tests cannot run in parallel.
  Test / parallelExecution := false,
  Test / fork := true,
  // A forked test JVM would start in its subproject's directory; pin it to the
  // root so that paths like versions.json mean the same thing from either
  // backend.
  Test / baseDirectory := (ThisBuild / baseDirectory).value,
  Test / javaOptions ++= jvmOptions,
  // Spark resolves the machine's hostname while `Utils` loads — before any
  // session config can be applied — and warns twice when it maps to loopback,
  // which on a laptop it always does. This is read early enough to prevent it,
  // and the forked test JVM is exactly the right scope for it.
  Test / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1",
  run / fork := true,
  run / javaOptions ++= jvmOptions,
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint")
)

/** Compile-time translation of a typed lambda into a `Column`.
  *
  * Its own subproject because Scala 2 macros cannot be used in the compilation unit that defines
  * them: the macro has to be compiled and on the classpath before anything expands it.
  *
  * It depends on `spark-sql-api` rather than on either client. That module is where Spark 4 put the
  * shared API — `Column` and `functions` live there — so the macro compiles once and works against
  * classic and connect alike, which is the whole claim being tested.
  */
lazy val macros = (project in file("macros"))
  .settings(
    name := "dev-nix-sail-scala-macros",
    libraryDependencies ++= Seq(
      "org.scala-lang" % "scala-reflect" % scalaVersion.value,
      "org.apache.spark" %% "spark-sql-api" % sparkVersion % Provided
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked")
  )

/** Classic Spark on the local JVM. The equivalent of `SPARK_BACKEND=pyspark`. */
lazy val classic = (project in file("backend/classic"))
  .dependsOn(macros)
  .settings(common)
  .settings(
    name := "dev-nix-sail-scala-classic",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
  )

/** The Spark Connect client, which is how you talk to Sail from the JVM. The equivalent of
  * `SPARK_BACKEND=pysail`.
  */
lazy val connect = (project in file("backend/connect"))
  .dependsOn(macros)
  .settings(common)
  .settings(
    name := "dev-nix-sail-scala-connect",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion,
      // Starting a Sail server from the JVM is not this template's job. The
      // kit does it, and using it here is also how we find out whether its
      // API is pleasant from the outside.
      "com.devel0pez" %% "sail-testkit" % versionOf("testkit") % Test
    )
  )

lazy val root = (project in file("."))
  .aggregate(macros, classic, connect)
  .settings(
    name := "dev-nix-sail-scala",
    publish / skip := true
  )
