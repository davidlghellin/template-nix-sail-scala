package devel0pez

import java.util.Properties
import java.util.concurrent.{ConcurrentHashMap, ConcurrentLinkedQueue, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Whether `FAIR` actually does anything, measured rather than asserted.
  *
  * `SchedulerSpec` proves the mechanism — the mode is set at creation, the pool is a thread-local,
  * the pool reaches the scheduler. What it deliberately does not show is that any of it *helps*,
  * and a scaladoc claiming a benefit nobody measured is exactly what this repository does not do.
  *
  * So: two jobs of eight slow tasks each, submitted from two threads onto two cores, with the same
  * code run under each mode. A `SparkListener` records which job every task belonged to, in the
  * order the tasks started, and the metric is how often that sequence **switches** between jobs.
  *
  * {{{
  * FIFO -> AABBBBBBBBAAAAAABA      4 switches
  * FAIR -> AABABABABAABABBBAB     13 switches
  * }}}
  *
  * Note that the metric is an ordering of events, not a duration. A timing assertion — "FAIR
  * finishes sooner" — would be the flaky version of this, and it would also be false: fair sharing
  * does not make the total faster, it stops one job monopolising the slots.
  *
  * This is still the most fragile test here, and it is worth knowing how much margin it has.
  * Measured over five runs: FIFO 3–4 switches, FAIR 11–13. The assertions below sit in the gap,
  * which is wide, and the relative one would survive even a much noisier machine.
  *
  * It is also the slowest, at roughly eight seconds: the tasks sleep on purpose, because tasks that
  * finish instantly never contend for a slot and there is nothing to schedule fairly.
  *
  * ==When submitting concurrently is worth the trouble==
  *
  * Interleaving is what `FAIR` does; whether you should be submitting several queries at once in
  * the first place is a different question, and it has a measurable answer. Four independent
  * queries with a shuffle, run serially and then concurrently on `local[12]`:
  *
  * {{{
  * rows per query     serial    concurrent    speedup
  *        500 000     397 ms        167 ms      2.38x
  *      5 000 000     250 ms        119 ms      2.10x
  *     50 000 000     433 ms        298 ms      1.45x
  *    200 000 000    1086 ms        921 ms      1.18x
  * }}}
  *
  * The gain decays, and the reason says what the technique is actually for. A query with a shuffle
  * does not keep twelve cores busy from start to finish: there is planning on the driver, a
  * boundary between stages, task launch, and a final stage of a hundred groups that occupies four
  * slots out of twelve. Cores idle through all of it. Running four queries at once lets one fill
  * another's gaps — so what concurrency buys is **the gaps**, not extra parallelism.
  *
  * Which is why the speedup shrinks as each query grows. Once a single query keeps every core busy
  * through long stages, the gaps are a rounding error and there is nothing left to fill.
  *
  * The rule that falls out: it pays when a job has **several independent outputs of moderate size**
  * — which is what an ETL with parallel branches looks like — and barely at all when it has one
  * enormous query. And `FAIR` is what makes it safe rather than what makes it fast: the parallelism
  * is yours, created with threads; the scheduler only stops the first job monopolising the slots,
  * which is what the tests below measure.
  *
  * Two limits on those numbers. They are one node with no I/O, and a real cluster reading from disk
  * has larger gaps, not smaller — so this probably understates the gain. And nothing here asserts
  * them: a wall-clock assertion measures the machine and the afternoon, not the scheduler.
  */
final class SchedulerFairnessSpec extends AnyFreeSpec with Matchers {

  /** Run the same two competing jobs under `mode` and return the order tasks started in.
    *
    * The session is built per call because `spark.scheduler.mode` is a static conf: the only way to
    * compare the two is two contexts. Any session left over from another suite is stopped first,
    * since `getOrCreate` would otherwise hand back one with the wrong mode.
    */
  private def taskOrderUnder(mode: String): String = {
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName(s"fairness-$mode")
      .config("spark.scheduler.mode", mode)
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // A task event carries a stage, not a job, so the mapping has to be built
    // from the job starts as they arrive.
    val stageToJob = new ConcurrentHashMap[Int, String]()
    val started = new ConcurrentLinkedQueue[String]()
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        val properties = Option(event.properties).getOrElse(new Properties())
        val label = Option(properties.getProperty("spark.job.description")).getOrElse("?")
        event.stageIds.foreach(id => stageToJob.put(id, label))
      }
      override def onTaskStart(event: SparkListenerTaskStart): Unit =
        started.add(Option(stageToJob.get(event.stageId)).getOrElse("?"))
    })

    val pool = Executors.newFixedThreadPool(2)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
    try {
      val session = spark
      import session.implicits._
      def competing(label: String) = Future {
        session.sparkContext.setJobDescription(label)
        // Eight partitions on two cores, and a task that takes long enough to
        // hold its slot. Without the sleep every task is over before the next
        // is scheduled and both modes look identical.
        session.range(0, 8, 1, 8).map { i => Thread.sleep(250); i }.count()
      }
      val first = competing("A")
      // A goes in clearly first, so that under FIFO it has every reason to keep
      // the slots. Submitting them at the same instant would make the FIFO
      // result depend on which thread won the race.
      Thread.sleep(120)
      val second = competing("B")
      Await.result(Future.sequence(Seq(first, second)), 120.seconds)
    } finally {
      pool.shutdown()
      spark.stop()
    }
    started.asScala.mkString
  }

  /** How often the sequence changes job — the whole metric. */
  private def switches(order: String): Int =
    order.sliding(2).count(pair => pair.length == 2 && pair(0) != pair(1))

  "two jobs competing for two cores" - {

    "queue behind each other under FIFO" in {
      val order = taskOrderUnder("FIFO")
      withClue(s"task order was $order: ") {
        // Long runs of one job: it holds the slots and the other waits.
        switches(order) should be <= 6
      }
    }

    "and interleave under FAIR, which is the entire point of it" in {
      val order = taskOrderUnder("FAIR")
      withClue(s"task order was $order: ") {
        switches(order) should be >= 9
      }
    }

    "with the difference large enough not to be a coincidence" in {
      // The assertion that would survive a slower or busier machine: the
      // absolute thresholds above are convenience, this is the claim.
      val fifo = switches(taskOrderUnder("FIFO"))
      val fair = switches(taskOrderUnder("FAIR"))
      withClue(s"FIFO=$fifo FAIR=$fair: ") {
        fair should be > fifo
      }
    }
  }
}
