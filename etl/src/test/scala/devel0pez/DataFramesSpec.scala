package devel0pez

import org.apache.spark.sql.functions.col

final class DataFramesSpec extends SparkSuite {

  "DataFrames.sumColumns" - {
    "adds the column holding the sum" in {
      val df = spark.range(1, 4).selectExpr("id as a", "id * 10 as b")

      val output = DataFrames.sumColumns(df, "a", "b", "total")

      output.columns should contain theSameElementsAs Seq("a", "b", "total")
      output.orderBy("a").collect().map(_.getAs[Long]("total")).toSeq shouldBe Seq(11L, 22L, 33L)
    }

    "keeps the original columns" in {
      val df = spark.range(1, 2).selectExpr("id as a", "id as b", "'x' as label")

      val output = DataFrames.sumColumns(df, "a", "b", "total")

      output.columns should contain("label")
    }

    "does not change the row count" in {
      val df = spark.range(1, 6).selectExpr("id as a", "id as b")

      DataFrames.sumColumns(df, "a", "b", "total").count() shouldBe df.count()
    }
  }

  "DataFrames.addColumns" - {

    "adds every column of the map in one go" in {
      val df = spark.range(1, 4).selectExpr("id as a", "id * 10 as b")

      val output = DataFrames.addColumns(
        df,
        Map("total" -> (col("a") + col("b")), "double" -> (col("a") * 2))
      )

      output.columns should contain theSameElementsAs Seq("a", "b", "total", "double")
      output.orderBy("a").collect().map(_.getAs[Long]("total")).toSeq shouldBe Seq(11L, 22L, 33L)
    }

    "says the same thing as the select that replaces it" in {
      val df = spark.range(1, 4).selectExpr("id as a", "id * 10 as b")

      val added = DataFrames.addColumns(df, Map("total" -> (col("a") + col("b"))))
      val selected = df.select(col("a"), col("b"), (col("a") + col("b")).as("total"))

      // Same columns, same order, same rows: when the list is known, the
      // `select` is the form to prefer, and this is what makes that claim
      // rather than the comment above it.
      added.columns.toSeq shouldBe selected.columns.toSeq
      added.orderBy("a").collect().toSeq shouldBe selected.orderBy("a").collect().toSeq
    }
  }
}
