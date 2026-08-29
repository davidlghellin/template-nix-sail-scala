package devel0pez

import scala.util.Try

import org.scalatest.matchers.should.Matchers

/** Vocabulary for the handful of places where the two engines genuinely differ.
  *
  * The tempting move when a spec fails on one backend is to mark it skipped — `ignore`, `pending`,
  * `cancel`. It goes yellow and the build is green again. The cost is that it goes **quiet
  * forever**: the day Sail closes the gap, nothing tells you, and the skip outlives the reason for
  * it by years.
  *
  * So nothing here skips. Each helper asserts what the backend it is running on actually does,
  * which keeps both arms live: if either engine changes, the stale arm fails and names itself. A
  * red test is how this template finds out that Sail caught up.
  */
trait EngineDivergence extends Matchers {

  /** Which engine this run is against. Supplied by whichever `SparkSuite` is on the classpath. */
  protected def backend: String

  /** Assert both realities: `classic` on the JVM engine, `sail` over Connect.
    *
    * For the cases where the two disagree about *how* they behave — different error class, NULL
    * against a raise — rather than about whether something works at all.
    */
  protected def perEngine[A](classic: => A)(sail: => A): A =
    if (backend == "classic") classic else sail

  /** `body` is expected to work on classic and to **fail on Sail**.
    *
    * The failure is asserted, not tolerated. `message`, when given, pins the reason down, so the
    * test distinguishes "Sail cannot do this" from "Sail broke in some other way" — and turns red
    * on the happy day Sail can do it.
    */
  protected def failsOnSail(message: String = "")(body: => Any): Unit = {
    val outcome = Try(body)
    if (backend == "classic") {
      withClue(s"expected this to work on classic, but it failed: ${outcome.failed.toOption}") {
        outcome.isSuccess shouldBe true
      }
    } else {
      withClue("expected this to fail on Sail; if it now works, delete the expectation") {
        outcome.isFailure shouldBe true
      }
      if (message.nonEmpty) outcome.failed.get.getMessage should include(message)
    }
  }
}
