package devel0pez

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

/** Demo on classic Spark: starts a local session and runs the base case. */
object Main {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .appName("dev-nix-sail-scala-classic")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try
      Demo.run(
        spark,
        "classic",
        spark.createDataFrame(BaseCase.Rows1.asJava, BaseCase.Table1),
        spark.createDataFrame(BaseCase.Rows2.asJava, BaseCase.Table2)
      )
    finally spark.stop()
  }
}
