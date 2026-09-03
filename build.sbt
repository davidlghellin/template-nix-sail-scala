ThisBuild / scalaVersion := versionOf("scala")
ThisBuild / version := "0.1.0"
ThisBuild / organization := "devel0pez"

// sbt forks its JVMs from the one that launched it, not from JAVA_HOME, and
// nixpkgs packages sbt with a JDK of its own. The devshell has to win.
ThisBuild / javaHome := sys.env.get("JAVA_HOME").map(file)

// Single source of truth, read by flake.nix too: Sail takes its Spark
// configuration from the client version, so the two cannot drift apart.
def versionOf(key: String): String = {
  val json = IO.read(file("versions.json"))
  val re = ("\"" + key + "\"\\s*:\\s*\"([^\"]+)\"").r
  re.findFirstMatchIn(json).map(_.group(1)).getOrElse {
    sys.error(s"versions.json has no key '$key'")
  }
}

val sparkVersion = versionOf("spark")
val backend = sys.env.getOrElse("SPARK_BACKEND", "classic")
val scalaTestVersion = "3.2.19"

// Spark and Arrow reach into JDK internals by reflection, sealed since Java 17.
// `spark-submit` adds these for you; sbt does not, and the first `collect()`
// dies with InaccessibleObjectException or Arrow's "sun.misc.Unsafe not
// available". Needed at run time, so `javaOptions` rather than `scalacOptions`.
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

// Both clients on one classpath, and the backend chosen at run time:
//
//   sbt test                          # classic
//   SPARK_BACKEND=connect sbt test    # Sail, over Spark Connect
//
// They coexist because the `org.apache.spark.sql.SparkSession` both ship is the
// same class repackaged from `spark-sql-api`; the implementations are
// `sql.classic.SparkSession` and `sql.connect.SparkSession`. `SparkSuite` names
// the concrete builder, because the generic one picks classic and then refuses
// `.remote(...)`.
lazy val common = Seq(
  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-sql" % sparkVersion,
    "org.apache.spark" %% "spark-connect-client-jvm" % sparkVersion,
    "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
    "org.typelevel" %% "cats-core" % "2.13.0" % Test,
    "com.devel0pez" %% "sail-testkit" % versionOf("testkit") % Test
  ),
  // One SparkSession per JVM.
  Test / parallelExecution := false,
  Test / fork := true,
  // A forked JVM starts in its own directory; pin it so `versions.json` and
  // `resources/` mean the same thing from every module.
  Test / baseDirectory := (ThisBuild / baseDirectory).value,
  Test / javaOptions ++= jvmOptions,
  // Read while `Utils` loads, before any session config applies.
  Test / envVars += "SPARK_LOCAL_IP" -> "127.0.0.1",
  // A forked JVM does not inherit the shell's environment.
  Test / envVars += "SPARK_BACKEND" -> sys.env.getOrElse("SPARK_BACKEND", "classic"),
  // The Sail server binary. The devshell puts `.venv-sail/bin` on PATH, but sbt
  // started by an IDE's build server may not have inherited it, and the testkit
  // then fails with "Could not run 'sail'". Naming the binary outright works
  // whatever launched sbt; an explicit SAIL_BIN still wins.
  Test / envVars ++= {
    val bundled = (ThisBuild / baseDirectory).value / ".venv-sail" / "bin" / "sail"
    sys.env
      .get("SAIL_BIN")
      .orElse(if (bundled.exists()) Some(bundled.getAbsolutePath) else None)
      .map(path => Map("SAIL_BIN" -> path))
      .getOrElse(Map.empty[String, String])
  },
  run / fork := true,
  run / javaOptions ++= jvmOptions,
  scalacOptions ++= Seq("-deprecation", "-feature", "-unchecked", "-Xlint")
)

/** Compile-time translation of a typed lambda into a `Column`.
  *
  * Its own module because a Scala 2 macro cannot be used in the unit that defines it. Built against
  * `spark-sql-api`, so it works against either client.
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

/** The domain, and every spec that runs against **both** engines. */
lazy val etl = (project in file("etl"))
  .dependsOn(macros)
  .settings(common)
  .settings(name := "dev-nix-sail-scala-etl")

/** Classic only: the local-JVM entry point, and the specs that reach for `SparkContext` or
  * `queryExecution` — neither of which the Connect client has.
  */
lazy val templateClassic = (project in file("template-classic"))
  .dependsOn(macros, etl, etl % "test->test")
  .settings(common)
  .settings(
    name := "dev-nix-sail-scala-classic",
    // Runs only under its own backend. These specs are not "skipped when
    // inconvenient" — they are about a client the other run does not have.
    Test / testOptions ++= (if (backend == "classic") Nil else Seq(Tests.Filter(_ => false)))
  )

/** Sail only: the Connect entry point, the request-protobuf reader and the client-side closure
  * guard, with their specs.
  */
lazy val templateConnect = (project in file("template-connect"))
  .dependsOn(macros, etl, etl % "test->test")
  .settings(common)
  .settings(
    name := "dev-nix-sail-scala-connect",
    Test / testOptions ++= (if (backend == "connect") Nil else Seq(Tests.Filter(_ => false)))
  )

lazy val root = (project in file("."))
  .aggregate(macros, etl, templateClassic, templateConnect)
  .settings(
    name := "dev-nix-sail-scala",
    publish / skip := true
  )
