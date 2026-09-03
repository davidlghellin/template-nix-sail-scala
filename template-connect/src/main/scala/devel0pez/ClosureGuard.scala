package devel0pez

import org.apache.spark.connect.proto
import org.sparkproject.io.grpc.{
  CallOptions,
  Channel,
  ClientCall,
  ClientInterceptor,
  ForwardingClientCall,
  MethodDescriptor
}

/** Raised by `ClosureGuard`, naming the operation and the line that wrote it. */
final class ClosureNotSupported(message: String) extends UnsupportedOperationException(message)

/** Refuses a plan carrying a Scala closure **before it leaves the client**.
  *
  * This is not an optimisation and it is worth being clear about why, because the obvious idea —
  * rewrite the plan on its way out to make it faster — does not work. The Connect client sends
  * plans *unresolved*; that is the design, not an oversight. Collapsing two projections, to take
  * the tempting example, means substituting one expression into another, and knowing whether
  * `col("b")` refers to the column just added or one that was already there needs the schema. The
  * client does not have it. So the plan level is the wrong altitude for the client to optimise at,
  * and the right one is the API level — which is where `Expr` operates, on the Scala AST, while the
  * types are still there.
  *
  * What the wire *is* good for is refusing early and saying why. A typed closure reaches Sail as a
  * `ScalarScalaUdf` whose argument is a wildcard carrying a plan id, and Sail's resolver resolves
  * arguments before it inspects the function, so it fails on the wildcard and reports
  * `[UNRESOLVED_WILDCARD_WITH_PLAN_ID]` — which names neither the closure nor the reason.
  * `WirePlanSpec` shows that shape in the request itself.
  *
  * The guard reads the same request one step earlier, where two things are still true that are not
  * true on the server: the operation is named (`filter`, `map`), and the client attached a JVM
  * origin, so the message can point at the user's own file and line. Neither is recoverable
  * server-side at any effort, because the server never had them.
  *
  * Install it on the session:
  *
  * {{{
  * SparkSession.builder().remote(url).interceptor(new ClosureGuard).create()
  * }}}
  *
  * It costs a walk of the request protobuf per `ExecutePlan` call, which is cheap next to the round
  * trip it replaces — and cheaper still than the round trip it prevents.
  */
final class ClosureGuard extends ClientInterceptor {

  override def interceptCall[Req, Res](
      method: MethodDescriptor[Req, Res],
      options: CallOptions,
      next: Channel
  ): ClientCall[Req, Res] =
    new ForwardingClientCall.SimpleForwardingClientCall[Req, Res](next.newCall(method, options)) {
      override def sendMessage(message: Req): Unit = {
        message match {
          // `ExecutePlanRequest` is the one that runs something. `AnalyzePlanRequest`
          // carries plans too — `schema`, `explain` — and is deliberately left alone:
          // asking a plan about itself is not the same as asking for it to be run, and
          // a guard that blocked both would break `explain` on the very plan whose
          // problem the user is trying to look at.
          case request: proto.ExecutePlanRequest => check(request.getPlan)
          case _                                 => ()
        }
        super.sendMessage(message)
      }
    }

  private def check(plan: proto.Plan): Unit = {
    val closures = WirePlan.closures(plan)
    if (closures.nonEmpty) {
      throw new ClosureNotSupported(
        s"${closures.map(_.toString).mkString("; ")}. " +
          "Sail has no JVM to run it on. Rewrite it with Columns — which also recovers the " +
          "predicate pushdown and the column pruning the closure gives up on classic Spark " +
          "(see PushdownSpec). `Expr.filterExpr` / `Expr.mapExpr` do the common cases."
      )
    }
  }
}

object ClosureGuard {

  /** Install a guard on a session builder.
    *
    * This exists because `SparkSession.Builder.interceptor` **cannot be called from Scala source**
    * in this Spark version, and the reason is worth writing down rather than working around
    * silently.
    *
    * Spark compiles the Connect client against gRPC and then shades it, rewriting the bytecode so
    * the method really takes `org.sparkproject.io.grpc.ClientInterceptor` — `javap` confirms it.
    * What the shading step does not rewrite is the `ScalaSignature` annotation, which still
    * describes the parameter as the original `io.grpc.ClientInterceptor`. So `scalac` reads the
    * stale signature, goes looking for a class that is on no classpath anywhere, and reports
    * `Symbol 'type io.grpc.ClientInterceptor' is missing`. Putting unshaded gRPC on the compile
    * path would not fix it either: it would typecheck and then emit a call to a method that does
    * not exist at run time.
    *
    * Reflection sidesteps the signature and binds against the descriptor that is actually there.
    * Java callers are unaffected, since they never read a `ScalaSignature` in the first place.
    */
  def install(
      builder: org.apache.spark.sql.connect.SparkSession.Builder,
      guard: ClosureGuard = new ClosureGuard
  ): org.apache.spark.sql.connect.SparkSession.Builder =
    builder.getClass
      .getMethod("interceptor", classOf[ClientInterceptor])
      .invoke(builder, guard)
      .asInstanceOf[org.apache.spark.sql.connect.SparkSession.Builder]
}
