package devel0pez

import com.devel0pez.sail.testkit.SailServer
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** One suite, either engine, chosen at run time.
  *
  * `SPARK_BACKEND=classic` (the default) runs on classic Spark in this JVM; `SPARK_BACKEND=connect`
  * starts a Sail server and talks to it over Spark Connect. Every spec that extends this therefore
  * runs twice — `sbt test` once per value — which is what the whole template is for.
  *
  * This used to be **two** classes with the same name, one per backend, because `build.sbt` held
  * that `spark-sql` and `spark-connect-client-jvm` could not share a classpath: both ship
  * `org.apache.spark.sql.SparkSession`. Measured, that is not true of Spark 4.2 — the class they
  * both ship is byte-for-byte the same one, repackaged from `spark-sql-api`, and the two
  * implementations live at `org.apache.spark.sql.classic.SparkSession` and
  * `org.apache.spark.sql.connect.SparkSession`. They coexist happily.
  *
  * There is one real catch, and it is why the builders below are spelled out in full. The generic
  * `SparkSession.builder()` resolves to **classic** when both clients are present, and refuses a
  * remote with `spark.connect.remote configuration is not supported in Classic mode`. Naming the
  * concrete builder is what sidesteps that — and it is also why `sail-testkit`'s `SailSuite` mixin
  * is not used here, since it calls the generic one. Its `SailServer` is used directly instead.
  */
trait SparkSuite extends AnyFreeSpec with Matchers with BeforeAndAfterAll with EngineDivergence {

  /** Which engine this run is against.
    *
    * `SPARK_BACKEND` by default, so the same spec runs both ways. A spec that only makes sense
    * against one engine **overrides it** rather than demanding the variable be set — it already
    * knows the answer, and requiring the caller to repeat it means a gutter click in the IDE fails
    * with a `ClassCastException` about a `Dataset` that says nothing about the environment.
    */
  protected val backend: String = sys.env.getOrElse("SPARK_BACKEND", "classic")

  protected var spark: SparkSession = _

  private var server: SailServer = _

  /** The Sail server backing `spark`, for the specs that need its url. Only on `connect`. */
  protected def sailServer: SailServer = {
    if (server == null) {
      throw new IllegalStateException(s"No Sail server: backend is '$backend', not 'connect'")
    }
    server
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    if (backend == "connect") {
      server = SailServer.start()
      // No `appName`: Connect ignores it and warns on every session.
      spark = org.apache.spark.sql.connect.SparkSession.builder().remote(server.url).create()
    } else {
      spark = org.apache.spark.sql.classic.SparkSession
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
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }

    // Set on both, and explicitly rather than left to the default: ANSI decides
    // whether a bad cast raises or returns NULL, and the two engines would be
    // answering different questions otherwise.
    spark.conf.set("spark.sql.ansi.enabled", "true")

    // No time zone override, deliberately. `TimeZoneSpec` measures why: a
    // timestamp the engine *computes* is where the backends part company,
    // because Sail reports the session zone back and then answers in UTC
    // anyway. Setting UTC here — which looks like it would make them agree —
    // shifts every `Timestamp.valueOf` fixture by a day instead.
  }

  override def afterAll(): Unit =
    try {
      if (spark != null) {
        try spark.stop()
        catch { case scala.util.control.NonFatal(_) => () }
      }
      // Last, and unguarded: a Sail server that is never closed is a child
      // process that outlives the run and keeps its port.
      if (server != null) server.close()
    } finally super.afterAll()
}
