package devel0pez

import org.apache.spark.sql.functions.col

/** Looking at the plan — the part of developing against Spark that nothing else substitutes for.
  *
  * Two things come out of it. The first is that the plan is where the two engines **stop pretending
  * to be the same**: everything else in this template is written once and passes on both, but a
  * plan is Catalyst's on one side and DataFusion's on the other, and they do not share a word of
  * vocabulary. The second is that a plan settles arguments — the `withColumn` measurement in
  * `ClassicPlanSpec` replaced a paragraph of this project's own documentation that was confidently
  * wrong.
  *
  * Only what both engines can express lives here. The logical plans are a **compile-time**
  * divergence, not a runtime one — Connect's `QueryExecution` is a different class without
  * `analyzed` or `optimizedPlan`, so code touching them does not build for the connect backend at
  * all, and no `perEngine` branch can rescue that. Those measurements live in `template-classic`,
  * which is what that module is for.
  */
final class PlanSpec extends SparkSuite {

  private def base = spark.range(1, 4).selectExpr("id as a")

  "a plan can be read on either engine" - {

    "but it is not written in the same language" in {
      val plan = Plans.of(base.filter(col("a") > 1))

      // Both start with the same header and then part company entirely.
      plan should include("== Physical Plan ==")
      perEngine {
        // Catalyst, with whole-stage codegen markers.
        plan should include("Filter")
        plan should include("Range")
      } {
        // Sail is DataFusion underneath, and its plan says so.
        plan should include("FilterExec")
        plan should include("RangeExec")
      }
    }

    "and only classic hands the plan back as an object" in {
      // `explain` prints on both; `queryExecution` is the richer door and it is
      // shut over Connect. Worth pinning: it is the API most plan-inspection
      // code reaches for first.
      failsOnSail("UNSUPPORTED_CONNECT_FEATURE")(base.queryExecution.toString)
    }
  }

  // Five columns added one at a time, versus the same five in a single
  // projection. This is the measurement behind `DataFrames.addColumns`.
  private def chained = (1 to 5).foldLeft(base)((df, i) => df.withColumn(s"c$i", col("a") + i))
  private def once = base.select(col("a") +: (1 to 5).map(i => (col("a") + i).as(s"c$i")): _*)

  "chained withColumn against one select" - {

    "produce the same physical plan, which is the surprising part" in {
      Plans.count(Plans.of(chained), "Project") shouldBe
        Plans.count(Plans.of(once), "Project")
    }

    "and give the same answer either way" in {
      chained.collect().map(_.toString).toSeq shouldBe once.collect().map(_.toString).toSeq
    }
  }
}
