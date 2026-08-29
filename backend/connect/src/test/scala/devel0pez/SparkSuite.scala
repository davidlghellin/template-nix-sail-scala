package devel0pez

import com.devel0pez.sail.testkit.SailSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Base for the tests that run against Sail, over Spark Connect.
  *
  * Starting and stopping the server is `sail-testkit`'s job, so this only says what is specific to
  * the template: which backend it is, and how the session is configured. Reimplementing the
  * lifecycle here — as this used to — meant two copies of it drifting apart.
  *
  * The specs that use it live in `shared/test` and are **the same ones** that run against classic
  * Spark: all that changes is where the session comes from. That both answer alike is precisely
  * what this template checks.
  */
trait SparkSuite extends AnyFreeSpec with Matchers with SailSuite with EngineDivergence {

  /** Backend name, for messages in the shared specs. */
  protected val backend: String = "connect"

  /** The kit configures nothing on purpose, so the project says what it wants. ANSI is set here to
    * the same value the classic suite sets, or the two backends would be answering different
    * questions.
    */
  override protected def configureSession(session: SparkSession): Unit =
    session.conf.set("spark.sql.ansi.enabled", "true")

  // No time zone override, and that is deliberate: the kit configures
  // nothing, and neither does the classic suite, so both backends read the
  // test data in the same zone the JVM built it in.
  //
  // Setting `spark.sql.session.timeZone = UTC` here — which looks like it
  // would make the two agree — makes them disagree instead: the fixtures use
  // `Timestamp.valueOf`, which is local time, so in UTC+1 the cutoff lands on
  // the previous day and a timestamp -> date cast comes back 2024-12-30.
  //
  // Override it when a project genuinely wants a fixed zone, and build the
  // fixtures in that zone too:
  //
  //   override protected def configureSession(session: SparkSession): Unit =
  //     session.conf.set("spark.sql.ansi.enabled", "true")
}
