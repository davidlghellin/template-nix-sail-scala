package devel0pez

import org.apache.spark.sql.{DataFrame, SparkSession}

/** The body of the demo, identical for both backends.
  *
  * It takes the session and the DataFrames already built, because that is the only thing that
  * differs between classic Spark and Connect; from here down the code is the same.
  */
object Demo {

  def run(spark: SparkSession, backend: String, t1: DataFrame, t2: DataFrame): Unit = {
    println(s"Spark ${spark.version} | backend: $backend")

    println("\n== source: TABLE_1 ==")
    t1.show(truncate = false)
    println("== source: TABLE_2 ==")
    t2.show(truncate = false)

    val output = BaseCase.etl(t1, t2, BaseCase.Cutoff, BaseCase.Audit)
    println("== output conformed to TABLE_OUT ==")
    output.show(truncate = false)
    output.printSchema()

    println("== summed columns ==")
    val numbers = spark.range(1, 4).selectExpr("id as a", "id * 10 as b")
    DataFrames.sumColumns(numbers, "a", "b", "total").show(truncate = false)

    println(s"== calculator: add(2, 3) = ${Calculator.add(2, 3)} ==")
  }
}
