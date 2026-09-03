package devel0pez

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.{SparkSession => ConnectSession}
import org.apache.spark.sql.functions.col

/** The guard, on a session of its own.
  *
  * It gets its own session because installing an interceptor is a property of the connection, not
  * something a running session can be talked into. That is also the honest shape of the feature: a
  * project adopts this at the point it builds its session, once, and every job it runs is covered.
  *
  * The session is built against the same server the suite already started, so this costs a gRPC
  * channel and no process.
  */
final class ClosureGuardSpec extends SparkSuite {

  /** Only meaningful against connect, so it says so instead of asking to be told. */
  override protected val backend: String = "connect"

  private var guarded: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The connect builder, not `sql.SparkSession.builder()`: `interceptor` is
    // specific to the Connect client and the shared API does not carry it. And
    // it goes on through `ClosureGuard.install`, not directly — see there for
    // why calling it from Scala does not compile.
    guarded = ClosureGuard.install(ConnectSession.builder().remote(sailServer.url)).create()
  }

  override def afterAll(): Unit =
    try if (guarded != null) guarded.close()
    finally super.afterAll()

  "with the guard installed" - {

    "a Column query is untouched" in {
      // The guard has to be invisible when there is nothing wrong, or nobody
      // will keep it switched on.
      guarded.range(10).filter(col("id") > 5).count() shouldBe 4
    }

    "a typed closure is refused before it reaches the server" in {
      val session = guarded
      import session.implicits._

      val refused = intercept[ClosureNotSupported] {
        guarded.range(10).filter(_ > 5L).count()
      }

      // Everything the server's own message could not say. The operation:
      refused.getMessage should include("`filter`")
      // the file and line that wrote it, which the server has never seen:
      refused.getMessage should include("ClosureGuardSpec.scala")
      // and what to do instead.
      refused.getMessage should include("Rewrite it with Columns")
    }

    "and the message it replaces named neither" in {
      val session = spark
      import session.implicits._

      // The same query on the ungarded session, for the comparison the guard
      // exists to make. This is what Sail says today.
      val raw = intercept[Exception](spark.range(10).filter(_ > 5L).count())

      raw.getMessage should include("wildcard with plan ID")
      raw.getMessage should not include "filter"
      raw.getMessage should not include "ClosureGuardSpec.scala"
    }
  }

  "what the guard deliberately does not do" - {

    "is block explain, which is how you look at the problem" in {
      val session = guarded
      import session.implicits._

      // `AnalyzePlanRequest` carries the offending plan too. Guarding it would
      // mean the one tool for inspecting a bad plan refused to run on bad
      // plans, so only `ExecutePlanRequest` is checked.
      val closures = WirePlan.closuresIn(guarded.range(10).filter(_ > 5L))
      closures.size shouldBe 1
    }
  }
}
