package devel0pez

import scala.util.Try

/** Every typed closure on `Dataset`, and what each engine does with it.
  *
  * The other specs meet this boundary one operation at a time, in the middle of an ETL. This one
  * takes the whole set at once, because the interesting fact is not that closures fail on Sail — it
  * is *how* they fail, and that they do not all fail alike.
  *
  * Four of the five die on `wildcard with plan ID`, which names neither the closure nor the reason.
  * `groupByKey` returns `Scala UDF is not supported yet`, which is accurate and which Sail has had
  * all along. That asymmetry is evidence, not trivia: it says the first four are aborting *before*
  * the check that produces the good message. In Sail's resolver, `resolve_map_partitions` resolves
  * the UDF's arguments before it inspects what kind of UDF it is, and a typed closure passes its
  * arguments as a wildcard with a plan id — so the wildcard fails first and the accurate branch is
  * never reached.
  *
  * This spec exists so that claim is checkable rather than remembered. If someone reorders that
  * resolver, the message for four of these changes and this test says so.
  */
final class ClosureSpec extends SparkSuite {

  private def sales = {
    val session = spark
    import session.implicits._
    Seq(
      Sale("ES", "0182", "P1", BigDecimal("100.50"), BaseCase.Cutoff),
      Sale("ES", "0227", "P9", BigDecimal("33.33"), BaseCase.Cutoff)
    ).toDS()
  }

  /** Each closure the typed API offers, run to completion so the plan is actually executed. */
  private def attempts: Map[String, Try[Any]] = {
    val session = spark
    import session.implicits._
    val ds = sales
    Map(
      "map" -> Try(ds.map(_.amount * 2).count()),
      "filter" -> Try(ds.filter(_.amount > 50).count()),
      "flatMap" -> Try(ds.flatMap(s => Seq(s.product, s.branch)).count()),
      "groupByKey" -> Try(ds.groupByKey(_.country).count().count()),
      "reduce" -> Try(ds.map(_.amount).reduce(_ + _))
    )
  }

  "the typed closures" - {

    "all run on classic, and none of them on Sail" in {
      val outcome = attempts

      perEngine {
        outcome.collect { case (name, result) if result.isFailure => name } shouldBe empty
      } {
        // Not "most of them" — every one. A closure is JVM bytecode, and the
        // far side has no JVM to run it with, so there is no partial support to
        // discover here.
        outcome.collect { case (name, result) if result.isSuccess => name } shouldBe empty
      }
    }

    "but Sail names the reason for exactly one of them" in {
      perEngine {
        // Nothing to compare on classic: they all succeeded above.
        attempts.values.count(_.isSuccess) shouldBe 5
      } {
        val messages = attempts.collect {
          case (name, result) if result.isFailure =>
            name -> result.failed.get.getMessage
        }

        // The accurate message, which Sail already has and almost never gets to use.
        messages("groupByKey") should include("Scala UDF is not supported yet")

        // And the four that abort earlier, on something that describes the plan
        // rather than the problem.
        // One assertion rather than a loop, so a failure names which of the four
        // stopped matching instead of stopping at the first.
        val notWildcard = Seq("map", "filter", "flatMap", "reduce")
          .filterNot(name => messages(name).contains("wildcard with plan ID"))

        withClue("these no longer report a wildcard, which is the good news: ") {
          notWildcard shouldBe empty
        }
      }
    }
  }

  "what to do instead" - {

    "is a column, and it works on both" in {
      val session = spark
      import session.implicits._
      import devel0pez.macros.Expr._

      // `map` and `filter` have column equivalents, and `ClosureSpec`'s point is
      // that reaching for them is not a concession — `PushdownSpec` measures the
      // closure costing a full table scan on the engine where it *did* run.
      sales.filterExpr(_.amount > 50).count() shouldBe 1L
      sales.mapExpr(_.branch).collect().toSet shouldBe Set("0182", "0227")
    }

    "except where it is not, and that is a real limit" in {
      // `flatMap`, `groupByKey` and `reduce` have no drop-in column form here.
      // `explode`, `groupBy`/`agg` and an aggregate expression cover most of
      // what people use them for, but "most" is doing work in that sentence —
      // a closure calling arbitrary Scala has nowhere to go.
      val session = spark
      import session.implicits._

      // The column form of `reduce(_ + _)`: an aggregate Catalyst can see into.
      sales
        .select(org.apache.spark.sql.functions.sum($"amount").as("total"))
        .as[BigDecimal]
        .head() shouldBe BigDecimal("133.83")
    }
  }
}
