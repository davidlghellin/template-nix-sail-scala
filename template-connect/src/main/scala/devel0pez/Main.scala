package devel0pez

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

/** Demo against Sail, over Spark Connect.
  *
  * It connects to a server that is already running instead of starting one. Starting it is what
  * `sail-testkit` does, and that is a **test** dependency: pulling a test kit into `src/main` to
  * run a demo would put it on the classpath of anything that ever depends on this template.
  *
  * sail spark server & # or `sail-server` inside the devshell SPARK_REMOTE=sc://localhost:50051 sbt
  * connect/run
  */
object Main {

  private val DefaultRemote = "sc://localhost:50051"

  def main(args: Array[String]): Unit = {
    val remote = sys.env.getOrElse("SPARK_REMOTE", DefaultRemote)
    println(s"connecting to $remote")

    val spark =
      try SparkSession.builder().remote(remote).getOrCreate()
      catch {
        case e: Throwable =>
          Console.err.println(
            s"""Could not reach a Spark Connect server at $remote.
               |Start one first:
               |    sail spark server          # `sail-server` in the devshell
               |and point SPARK_REMOTE at it if it is not on the default port.
               |Cause: ${e.getMessage}""".stripMargin
          )
          sys.exit(1)
      }

    try
      Demo.run(
        spark,
        "connect (Sail)",
        spark.createDataFrame(BaseCase.Rows1.asJava, BaseCase.Table1),
        spark.createDataFrame(BaseCase.Rows2.asJava, BaseCase.Table2)
      )
    finally spark.stop()
  }
}
