package devel0pez

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.sql.functions.col

/** Submitting several independent queries at once, on both engines.
  *
  * This is the half of `SchedulerSpec` that ports. The scheduler itself does not: `FAIR`, pools and
  * `setLocalProperty` all live on `SparkContext`, which the Connect client does not have, so those
  * specs are classic-only by construction. What survives the crossing is the question a person
  * actually has — *if my ETL has independent branches, can I run them at the same time, and does it
  * help?* — and that is answerable against either engine from the client side.
  *
  * Both say yes to the first part. Four queries submitted from four threads are genuinely in flight
  * together on both, and return exactly what they return one at a time.
  *
  * The second part is where they differ, and the numbers are worth writing down even though nothing
  * asserts them, because the shape of the difference is the interesting bit. Measured over three
  * runs, four `range().filter().count()` queries, warm:
  *
  * {{{
  *              serial        concurrent
  * classic   150-167 ms       51-55 ms     ~3x
  * Sail       17-23 ms        14-17 ms     ~1.2x
  * }}}
  *
  * Classic gets three times faster on a session with **one** executor thread, which is not
  * execution parallelism — there is none to be had. It is the driver work (parse, analyse,
  * optimise, plan) overlapping across threads, and on queries this small that is most of the cost.
  *
  * Which reframes Sail's more modest 1.2x: it is not that concurrency helps less there, it is that
  * its serial baseline is already eight times faster, so there is far less driver overhead left to
  * hide. The speedup classic gains is largely overhead Sail never paid.
  *
  * Timings are documented and not asserted, deliberately. A wall-clock assertion is the flaky
  * version of this, and it would be measuring the machine rather than the engines.
  */
final class ConcurrencySpec extends SparkSuite {

  private def query(scale: Int): Long =
    spark.range(0, 200000L * scale).filter(col("id") % 7 === 0).count()

  "four independent queries at once" - {

    "are genuinely in flight together, not serialised by the client" in {
      val pool = Executors.newFixedThreadPool(4)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      val inFlight = new AtomicInteger(0)
      val peak = new AtomicInteger(0)

      try {
        val work = Future.sequence((1 to 4).map { scale =>
          Future {
            peak.updateAndGet(_ max inFlight.incrementAndGet())
            try query(scale)
            finally inFlight.decrementAndGet()
          }
        })
        Await.result(work, 180.seconds) shouldBe Seq(28572L, 57143L, 85715L, 114286L)

        // Observed as 4 on every run of both engines. Asserted loosely because
        // the strict version is a race against the queries being fast: if one
        // finishes before the next thread is scheduled, the peak is lower
        // through no fault of the engine. Two is enough to show the client is
        // not taking a lock and serialising.
        withClue(s"peak concurrent queries was ${peak.get}: ") {
          peak.get should be >= 2
        }
      } finally pool.shutdown()
    }

    "and give the same answers as running them one at a time" in {
      // The part that would actually hurt if it were false, and the reason to
      // have this test at all rather than a benchmark.
      val pool = Executors.newFixedThreadPool(4)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        val serial = (1 to 4).map(query)
        val concurrent =
          Await.result(Future.sequence((1 to 4).map(scale => Future(query(scale)))), 180.seconds)
        concurrent shouldBe serial
      } finally pool.shutdown()
    }
  }
}
