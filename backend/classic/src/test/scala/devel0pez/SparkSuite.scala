package devel0pez

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Base for the tests that run on classic Spark, on the local JVM.
  *
  * The specs that use it live in `shared/test` and are **the same ones** that run against Sail: all
  * that changes is where the session comes from.
  */
trait SparkSuite extends AnyFreeSpec with Matchers with BeforeAndAfterAll with EngineDivergence {

  protected var spark: SparkSession = _

  /** Backend name, for messages in the shared specs. */
  protected val backend: String = "classic"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession
      .builder()
      .master("local[1]")
      .appName("test-classic")
      // Without this Spark opens 200 partitions to aggregate three rows.
      .config("spark.sql.shuffle.partitions", "1")
      .config("spark.ui.enabled", "false")
      // Nothing here needs a routable address. `SPARK_LOCAL_IP` in `build.sbt`
      // is what actually silences the hostname warning — it is read while
      // `Utils` loads, before any of this applies — but saying it twice costs
      // nothing and makes the intent local to the session.
      .config("spark.driver.bindAddress", "127.0.0.1")
      .config("spark.driver.host", "127.0.0.1")
      // Set explicitly rather than left to the default: ANSI decides whether a
      // bad cast raises or returns NULL, and a template should not have that
      // depend on which Spark version happens to be on the classpath.
      .config("spark.sql.ansi.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
  }

  override def afterAll(): Unit = {
    try if (spark != null) spark.stop()
    finally super.afterAll()
  }
}
