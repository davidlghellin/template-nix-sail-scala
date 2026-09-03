package devel0pez

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.Dataset
import org.sparkproject.com.google.protobuf.Message

/** A Scala closure found in a plan, with the source line that put it there. */
final case class ClosureSite(operation: String, callSite: Option[String]) {
  override def toString: String =
    callSite.fold(s"$operation with a Scala closure")(where =>
      s"$operation with a Scala closure at $where"
    )
}

/** The plan as the client would send it — read **without sending it**.
  *
  * `Plans.of` in the shared suite captures what `explain()` prints, which is the server's view
  * after it has parsed, resolved and optimised. That is the right tool for asking what an engine
  * *did*. It cannot answer what the client *asked for*, because by the time `explain` speaks, the
  * request has already been interpreted — and when the two engines disagree, which of them invented
  * the difference is exactly the question.
  *
  * `Dataset.plan` is the other end. It is the protobuf the client would put on the wire, available
  * locally, with no server involved and nothing executed. There is no classic equivalent and there
  * cannot be: on classic there is no wire.
  *
  * What it is for here is evidence. `ClosureSpec` records that four of the five typed closures die
  * on `wildcard with plan ID` while `groupByKey` gets the accurate `Scala UDF is not supported
  * yet`, and explains the asymmetry by reasoning about Sail's resolver. That explanation used to
  * rest on the error strings alone. `WirePlanSpec` now reads the request instead, and finds the
  * cause sitting in the proto: `filter` and `map` attach a `plan_id` to the wildcard they pass the
  * UDF, and `groupByKey` does not.
  */
object WirePlan {

  /** The request protobuf, unsent. */
  def of(ds: Dataset[_]): proto.Plan =
    ds.asInstanceOf[org.apache.spark.sql.connect.Dataset[_]].plan

  /** The same thing as text, which is protobuf's own debug format. */
  def text(ds: Dataset[_]): String = of(ds).toString

  /** Every Scala closure in the plan, named, with the line of code that added it. */
  def closuresIn(ds: Dataset[_]): Seq[ClosureSite] = closures(of(ds))

  /** As above, for a plan already in hand — which is how `ClosureGuard` reaches it. */
  def closures(message: Message): Seq[ClosureSite] = scalaUdfs(message).map(site)

  /** Walks the whole message tree by protobuf reflection rather than by matching each relation type
    * by hand.
    *
    * A closure can hang off a `filter` condition, a `map_partitions` func or an `aggregate`'s
    * grouping expressions, and that list grows with the protocol. Reflection finds them wherever
    * they are, and keeps this working against a Spark version that adds another.
    */
  private def scalaUdfs(message: Message): Seq[proto.CommonInlineUserDefinedFunction] = {
    val found = Seq.newBuilder[proto.CommonInlineUserDefinedFunction]

    def visit(node: Message): Unit = {
      node match {
        case udf: proto.CommonInlineUserDefinedFunction
            if udf.getFunctionCase ==
              proto.CommonInlineUserDefinedFunction.FunctionCase.SCALAR_SCALA_UDF =>
          found += udf
        case _ => ()
      }
      node.getAllFields.asScala.values.foreach {
        case child: Message => visit(child)
        case repeated: java.util.List[_] =>
          repeated.asScala.foreach {
            case child: Message => visit(child)
            case _              => ()
          }
        case _ => ()
      }
    }

    visit(message)
    found.result()
  }

  /** The operation and the user's call site, both read out of the JVM origin the client attaches.
    *
    * That origin is the reason a client-side message can beat anything the server could say: it
    * carries the file and line of the call, which the server has never seen and never will.
    */
  private def site(udf: proto.CommonInlineUserDefinedFunction): ClosureSite = {
    val frames = udf.getArgumentsList.asScala.headOption.toSeq
      .flatMap(_.getCommon.getOrigin.getJvmOrigin.getStackTraceList.asScala)

    val operation = frames
      .find(_.getDeclaringClass.startsWith("org.apache.spark"))
      .map(frame => s"`${frame.getMethodName}`")

    val callSite = frames
      .find(frame => !frame.getDeclaringClass.startsWith("org.apache.spark"))
      .map(frame => s"${frame.getFileName}:${frame.getLineNumber}")

    ClosureSite(operation.getOrElse("a typed operation"), callSite)
  }
}
