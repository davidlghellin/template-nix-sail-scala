package devel0pez

import org.apache.spark.sql.functions.col

/** Plan measurements that only classic Spark can express.
  *
  * This spec lives here rather than in `shared/test` for a reason worth knowing: Connect's
  * `QueryExecution` is a different class, without `analyzed` or `optimizedPlan`. Touching them is a
  * **compile** error for the connect backend, not a runtime failure, so no `perEngine` or
  * `failsOnSail` branch can hold it — the code simply must not be compiled against Connect. A
  * backend-specific source directory is the only thing that says that.
  *
  * What it measures is the claim behind `DataFrames.addColumns`, which this project got wrong in
  * its own documentation before anybody looked at a plan.
  */
final class ClassicPlanSpec extends SparkSuite {

  private def base = spark.range(1, 4).selectExpr("id as a")
  private def chained = (1 to 5).foldLeft(base)((df, i) => df.withColumn(s"c$i", col("a") + i))
  private def once = base.select(col("a") +: (1 to 5).map(i => (col("a") + i).as(s"c$i")): _*)

  private def projects(plan: Any): Int = Plans.count(plan.toString, "Project")

  "five chained withColumn against one select" - {

    "nest in the analyzed plan, which is the part that is true" in {
      projects(chained.queryExecution.analyzed) shouldBe 6
      projects(once.queryExecution.analyzed) shouldBe 2
    }

    "and are flattened by the optimizer, which is the part that surprises" in {
      // `CollapseProject`. The cost of a chain is therefore paid in planning,
      // by an analyzer walking six nodes instead of two on every operation
      // that follows — not in a worse query, because the query is the same.
      projects(chained.queryExecution.optimizedPlan) shouldBe 1
      projects(once.queryExecution.optimizedPlan) shouldBe 1
    }
  }
}
