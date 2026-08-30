package devel0pez

import org.apache.spark.connect.proto
import org.apache.spark.sql.functions.col

/** What the client puts on the wire, read before it is sent.
  *
  * Connect-only, and not for want of trying to share it: there is no classic equivalent of a
  * request protobuf, because on classic there is no request.
  *
  * The spec earns its place on the last two tests. `ClosureSpec` observes that Sail reports
  * `groupByKey` accurately and the other closures as an unresolved wildcard, and explains it by
  * reasoning about the order of Sail's resolver. Here that explanation stops being an inference:
  * the difference is visible in what the client sends, one step before any engine sees it.
  */
final class WirePlanSpec extends SparkSuite {

  private def base = spark.range(10)

  /** The one Scala closure in a plan, or a failure naming how many there were. */
  private def onlyUdf(ds: org.apache.spark.sql.Dataset[_]): proto.Expression = {
    val udfs = udfArguments(ds)
    withClue("expected exactly one Scala closure in the plan: ") { udfs.size shouldBe 1 }
    udfs.head
  }

  /** The first argument of every Scala closure in the plan — the wildcard the UDF is handed. */
  private def udfArguments(ds: org.apache.spark.sql.Dataset[_]): Seq[proto.Expression] = {
    def walk(node: org.sparkproject.com.google.protobuf.Message): Seq[proto.Expression] = {
      val here = node match {
        case udf: proto.CommonInlineUserDefinedFunction
            if udf.getFunctionCase ==
              proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
          Seq(udf.getArguments(0))
        case _ => Seq.empty
      }
      import scala.jdk.CollectionConverters._
      here ++ node.getAllFields.asScala.values.flatMap {
        case child: org.sparkproject.com.google.protobuf.Message => walk(child)
        case repeated: java.util.List[_] =>
          repeated.asScala.flatMap {
            case child: org.sparkproject.com.google.protobuf.Message => walk(child)
            case _                                                   => Seq.empty
          }
        case _ => Seq.empty
      }
    }
    walk(WirePlan.of(ds))
  }

  "the request the client would send" - {

    "is available without a server, because nothing has been executed yet" in {
      // The whole point of reading it here: no round trip, no engine, no plan id
      // allocated by anyone but the client.
      WirePlan.text(base.filter(col("id") > 5)) should include("unresolved_function")
    }

    "carries a Column filter as an expression the far side can read" in {
      val condition = WirePlan.of(base.filter(col("id") > 5)).getRoot.getFilter.getCondition

      condition.getUnresolvedFunction.getFunctionName shouldBe ">"
      WirePlan.closuresIn(base.filter(col("id") > 5)) shouldBe empty
    }

    "carries a typed closure as an opaque Scala UDF instead" in {
      val session = spark
      import session.implicits._

      val condition = WirePlan.of(base.filter(_ > 5L)).getRoot.getFilter.getCondition

      condition.getCommonInlineUserDefinedFunction.getFunctionCase shouldBe
        proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF

      // Nothing in there is an expression. It is serialised JVM bytecode, which
      // is why no amount of work on Sail's planner could see into it.
      condition.getUnresolvedFunction.getFunctionName shouldBe ""
    }
  }

  "the asymmetry ClosureSpec observes" - {

    "is a plan id on the wildcard, and filter attaches one" in {
      val session = spark
      import session.implicits._

      val star = onlyUdf(base.filter(_ > 5L)).getUnresolvedStar

      // This is the `wildcard with plan ID` Sail reports, sitting in the request.
      // Sail resolves a UDF's arguments before it inspects the UDF, so this is
      // what it trips over — and the accurate `Scala UDF is not supported yet`
      // branch is never reached.
      star.hasPlanId shouldBe true
    }

    "and groupByKey does not, which is why its message is the good one" in {
      val session = spark
      import session.implicits._

      val star = onlyUdf(base.groupByKey(v => v % 2).count()).getUnresolvedStar

      // Same closure, same UDF, no plan id. The wildcard resolves, execution
      // reaches the check that knows what a Scala UDF is, and the error finally
      // says so. One optional field is the whole difference between a message
      // that helps and one that does not.
      star.hasPlanId shouldBe false
    }
  }

  "a closure found in the request" - {

    "names the operation and the line of code that wrote it" in {
      val session = spark
      import session.implicits._

      val found = WirePlan.closuresIn(base.filter(_ > 5L))

      found.size shouldBe 1
      found.head.operation shouldBe "`filter`"
      // The client attaches a JVM origin to the expression. The server never
      // sees this file, which is why no server-side error can ever point here.
      found.head.callSite.getOrElse("") should include("WirePlanSpec.scala")
    }
  }
}
