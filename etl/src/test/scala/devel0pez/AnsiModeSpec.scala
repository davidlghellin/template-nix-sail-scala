package devel0pez

import org.apache.spark.sql.Row

/** ANSI mode, set for the whole suite and flipped inside a single test.
  *
  * It decides whether an invalid operation raises or quietly returns NULL, so it changes what a
  * query *means*. That is why `sail-testkit` leaves `configureSession` empty: a test kit picking
  * this for you would be answering a question nobody asked it.
  *
  * Two ways to reach it, and a template needs both: the suite-wide default (see `SparkSuite`) and
  * the per-test override below, for the one scenario that needs the opposite of everything else.
  */
final class AnsiModeSpec extends SparkSuite {

  private val divideByZero = "SELECT 1.0 / 0.0 AS result"

  /** Runs `body` with `ansi.enabled` forced, then puts the old value back.
    *
    * Restoring matters more than it looks. A test that leaves the flag flipped changes the meaning
    * of every test after it, and the failure turns up somewhere unrelated — which is a bad
    * afternoon.
    */
  private def withAnsi[A](enabled: Boolean)(body: => A): A = {
    val key = "spark.sql.ansi.enabled"
    val previous = scala.util.Try(spark.conf.get(key)).toOption
    spark.conf.set(key, enabled.toString)
    try body
    finally previous.foreach(spark.conf.set(key, _))
  }

  private def result(sql: String): Row = spark.sql(sql).collect().head

  "ANSI mode" - {

    "off, dividing by zero returns NULL" in {
      withAnsi(enabled = false) {
        // `isNullAt` rather than `shouldBe null`: `get(0)` is `Any`, and
        // ScalaTest only compares AnyRef against null.
        result(divideByZero).isNullAt(0) shouldBe true
      }
    }

    "on, the same division raises" in {
      withAnsi(enabled = true) {
        val attempt = scala.util.Try(result(divideByZero))

        attempt.isFailure shouldBe true
      }
    }

    "the flag goes back to what the suite set" in {
      withAnsi(enabled = false)(())

      spark.conf.get("spark.sql.ansi.enabled") shouldBe "true"
    }
  }
}
