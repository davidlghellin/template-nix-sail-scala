package devel0pez

import java.sql.{Date, Timestamp}
import java.time.{Instant, ZoneId}

import org.apache.spark.sql.functions.{col, lit, timestamp_seconds}

/** What a timestamp means, and the one place the two engines quietly disagree about it.
  *
  * The connect `SparkSuite` carries a comment explaining why it does not set
  * `spark.sql.session.timeZone`. That comment had been sitting there unverified, which in this
  * project is a bug in itself: a hazard worth writing down is a hazard worth pinning. Writing the
  * test found something the comment did not know about.
  *
  * The divergence is the second block. It is not a crash and not an error class — it is the same
  * query returning a different date, which is the kind this template exists to catch.
  *
  * Nothing here depends on the machine's zone. An assertion phrased against local time passes in
  * Madrid and is vacuous on a CI runner in UTC; every zone below is named, and the instant is fixed
  * by definition.
  */
final class TimeZoneSpec extends SparkSuite {

  /** 23:30 UTC, chosen so that anywhere east of Greenwich has already rolled over. */
  private val instant = Instant.parse("2026-01-19T23:30:00Z")

  /** Set the session zone for the duration of `body`, then put it back.
    *
    * Safe only because the suites run single-threaded (`Test / parallelExecution := false`): this
    * is session-global state, so it would otherwise be a race with every other spec.
    */
  private def withZone[A](zone: String)(body: => A): A = {
    val key = "spark.sql.session.timeZone"
    val previous = spark.conf.get(key)
    try {
      spark.conf.set(key, zone)
      body
    } finally spark.conf.set(key, previous)
  }

  /** A timestamp the engine computes for itself, from an absolute instant. */
  private def computed: Date =
    spark
      .range(1)
      .select(timestamp_seconds(lit(instant.getEpochSecond)))
      .toDF("ts")
      .select(col("ts").cast("date"))
      .collect()
      .head
      .getDate(0)

  /** The literal every fixture in this project is built from. */
  private val fixture = "2026-01-19 00:30:00"

  /** What `java.time` says that fixture's instant falls on, in a given zone.
    *
    * Derived rather than written down, and that is the whole point of it. `Timestamp.valueOf`
    * resolves its string in the **JVM** zone, so the instant differs between a laptop in Madrid and
    * a CI runner in UTC. An expected value typed as a literal is therefore a value for one machine
    * — which is exactly the mistake this spec was written to warn about, and made anyway in its
    * first version.
    */
  private def expected(zone: String): Date =
    Date.valueOf(Timestamp.valueOf(fixture).toInstant.atZone(ZoneId.of(zone)).toLocalDate)

  /** A timestamp that travels in the data, built the way every fixture here is built. */
  private def carried: Date = {
    val session = spark
    import session.implicits._
    Seq(Timestamp.valueOf("2026-01-19 00:30:00"))
      .toDF("ts")
      .select(col("ts").cast("date"))
      .collect()
      .head
      .getDate(0)
  }

  "a timestamp carried in the data" - {

    "reads the same on both engines, which is why the fixtures here are safe" in {
      // Both engines apply the session zone to a stored timestamp the way
      // `java.time` would, so every `Timestamp.valueOf` fixture in this project
      // means the same thing to classic and to Sail. Measured across four
      // zones; two are kept.
      withZone("UTC")(carried) shouldBe expected("UTC")
      withZone("Asia/Tokyo")(carried) shouldBe expected("Asia/Tokyo")
    }

    "and moves by a day under UTC, which is the trap the suites avoid" in {
      // `Timestamp.valueOf` reads its string in the **JVM** zone, so a fixture
      // is pinned to the machine while the session zone decides how it is read
      // back. Change the session zone and the value moves. Hence: no zone
      // override in either suite, and fixtures built in whatever zone the JVM
      // is in.
      //
      // These two are 25 hours apart, so they disagree about the date for *any*
      // instant. Naming a zone the JVM might already be in would make the test
      // pass or vanish depending on the machine.
      withZone("Pacific/Kiritimati")(carried) should not be withZone("Pacific/Midway")(carried)
    }
  }

  "a timestamp the engine computes" - {

    "is where the two genuinely disagree, and nothing raises" in {
      // Same query, same instant, same session zone, different answer. The zone
      // is named rather than inherited from the JVM: 23:30 UTC is 08:30 the next
      // morning in Tokyo, on any machine.
      perEngine {
        withZone("Asia/Tokyo")(computed) shouldBe Date.valueOf("2026-01-20")
      } {
        // Sail answers in UTC and stays there. Note the config is not being
        // rejected or lost — `spark.conf.get` hands the zone straight back —
        // it simply is not applied to a timestamp the engine built itself.
        withZone("Asia/Tokyo")(computed) shouldBe Date.valueOf("2026-01-19")
      }
    }

    "and on Sail the session zone changes nothing at all" in {
      perEngine {
        // Classic tracks the zone, correctly, in every direction.
        withZone("UTC")(computed) shouldBe Date.valueOf("2026-01-19")
        withZone("Pacific/Kiritimati")(computed) shouldBe Date.valueOf("2026-01-20")
      } {
        // Sail gives the same date for UTC and for UTC+14. Measured also for
        // Asia/Tokyo and for the unset default: always the UTC answer.
        withZone("UTC")(computed) shouldBe Date.valueOf("2026-01-19")
        withZone("Pacific/Kiritimati")(computed) shouldBe Date.valueOf("2026-01-19")
      }
    }

    "so setting UTC explicitly is what makes the two agree" in {
      // The practical advice, and it cuts against the instinct. A pipeline that
      // fixes the session zone to UTC gets the same answer from both engines,
      // because that is the only zone Sail implements. The cost is the block
      // above: fixtures have to be built in UTC too, or they move.
      withZone("UTC")(computed) shouldBe Date.valueOf("2026-01-19")
    }
  }
}
