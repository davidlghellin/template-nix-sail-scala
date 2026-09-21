package devel0pez

import java.util.Properties
import java.util.concurrent.{ConcurrentLinkedQueue, Executors}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Running independent actions concurrently, and the fair scheduler that makes it worth doing.
  *
  * Classic-only, and not by a runtime branch: the whole mechanism lives in `SparkContext`, which
  * the Connect client does not have. `sc.setLocalProperty` is not a method you can call and get an
  * error from — there is no `sparkContext` to call it on, so a spec written against it does not
  * compile for the connect backend. That is the same category as `ClassicPlanSpec`, which is why
  * this sits beside it.
  *
  * The idea is worth stating precisely, because it promises less than people expect. `FAIR` does
  * **not** parallelise anything. One `collect()` is one job either way, and the DAG is not "broken
  * up". What it changes is what happens when *you* submit several independent actions from several
  * threads: under `FIFO` the first job takes every executor slot and the rest queue behind it;
  * under `FAIR` they share. The parallelism is yours to create with threads; the scheduler only
  * decides whether they get in each other's way.
  *
  * This session is its own, with four cores and `FAIR`, because neither can be changed on a live
  * context — which is itself one of the things asserted below.
  */
final class SchedulerSpec extends AnyFreeSpec with Matchers with BeforeAndAfterAll {

  private var spark: SparkSession = _
  private val jobPools = new ConcurrentLinkedQueue[(String, String)]()

  override def beforeAll(): Unit = {
    super.beforeAll()
    // A context already exists if another suite ran first, and `getOrCreate`
    // would hand it back with its own settings. Scheduler mode and core count
    // are fixed at creation, so the only way to have different ones is a
    // different context.
    SparkSession.getActiveSession.orElse(SparkSession.getDefaultSession).foreach(_.stop())

    spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("test-scheduler")
      .config("spark.scheduler.mode", "FAIR")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.ui.enabled", "false")
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      .config("spark.sql.ansi.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // The only way to prove a pool assignment actually reached the scheduler,
    // rather than just sitting in a thread-local nobody read.
    spark.sparkContext.addSparkListener(new SparkListener {
      override def onJobStart(event: SparkListenerJobStart): Unit = {
        val properties = Option(event.properties).getOrElse(new Properties())
        jobPools.add(
          (
            Option(properties.getProperty("spark.job.description")).getOrElse("?"),
            Option(properties.getProperty("spark.scheduler.pool")).getOrElse("<default>")
          )
        )
      }
    })
  }

  override def afterAll(): Unit =
    try if (spark != null) spark.stop()
    finally super.afterAll()

  private def numbers(n: Int) = spark.range(n).toDF("id")

  "the scheduler mode" - {

    "is FIFO unless you ask for FAIR at session creation" in {
      // The default nobody changes, which is why the technique is rarer than
      // the problem it solves.
      spark.conf.get("spark.scheduler.mode") shouldBe "FAIR"
      new org.apache.spark.SparkConf().get("spark.scheduler.mode", "FIFO") shouldBe "FIFO"
    }

    "and cannot be changed once the context is running" in {
      // Worth measuring rather than assuming, because the assumption is the
      // pessimistic one: a setting that belongs to `SparkContext` could easily
      // be accepted by the SQL conf and then quietly ignored, which is what
      // several session settings in this repository do — see `TimeZoneSpec` for
      // one that Sail reports back and never applies.
      //
      // Spark does better here. `spark.scheduler.mode` is a *static* conf, and
      // changing it on a live session raises instead of pretending.
      val refused = intercept[org.apache.spark.sql.AnalysisException] {
        spark.conf.set("spark.scheduler.mode", "FIFO")
      }
      refused.getMessage should include("CANNOT_MODIFY_CONFIG")

      // So the mode is whatever the context was built with, and the only way to
      // have a different one is a different context.
      spark.sparkContext.getConf.get("spark.scheduler.mode") shouldBe "FAIR"
    }
  }

  "the pool a job lands in" - {

    "is a thread-local, so two threads can aim at two pools" in {
      val pool = Executors.newFixedThreadPool(2)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        val seen = Future.sequence(
          Seq("fast", "slow").map { name =>
            Future {
              spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)
              spark.sparkContext.getLocalProperty("spark.scheduler.pool")
            }
          }
        )
        Await.result(seen, 30.seconds) should contain theSameElementsAs Seq("fast", "slow")

        // And the calling thread never had one set: this is per-thread state,
        // not session state. It is also why a shared thread pool needs the
        // property set *inside* each task — set it once outside and the tasks
        // inherit whatever the last caller left behind.
        spark.sparkContext.getLocalProperty("spark.scheduler.pool") shouldBe null
      } finally pool.shutdown()
    }

    "and reaches the scheduler, which is the part worth proving" in {
      jobPools.clear()
      val pool = Executors.newFixedThreadPool(2)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        val work = Future.sequence(
          Seq("fast", "slow").map { name =>
            Future {
              spark.sparkContext.setLocalProperty("spark.scheduler.pool", name)
              spark.sparkContext.setJobDescription(s"job-$name")
              numbers(1000).filter(col("id") > 10).count()
            }
          }
        )
        Await.result(work, 60.seconds) shouldBe Seq(989L, 989L)

        // Read off the listener rather than off the thread-local: this is the
        // scheduler's own view of what it was asked to do.
        val byJob = jobPools.asScala.toMap
        byJob.get("job-fast") shouldBe Some("fast")
        byJob.get("job-slow") shouldBe Some("slow")
      } finally pool.shutdown()
    }
  }

  "several independent actions at once" - {

    "all complete and all give the right answer" in {
      // The part that is genuinely yours rather than the scheduler's: nothing
      // here happens concurrently unless you submit it concurrently. `FAIR`
      // only decides whether these share the executors or queue.
      val pool = Executors.newFixedThreadPool(4)
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(pool)
      try {
        val counts = Future.sequence(
          (1 to 4).map(i => Future(numbers(i * 100).count()))
        )
        Await.result(counts, 60.seconds) shouldBe Seq(100L, 200L, 300L, 400L)
      } finally pool.shutdown()
    }
  }
}
