package devel0pez

import java.sql.Timestamp

import org.apache.spark.sql.functions.col

import devel0pez.macros.Expr

/** The typed lambda that does not travel, and the two ways round it.
  *
  * Split out of `TypedEtlSpec`, which keeps the ETL. This is the part that is a fact about Sail
  * rather than a way to write a job, and it is also the only thing in the template that needed the
  * macro.
  */
final class TypedLambdaSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val salesTbl = s"lambda_sales_$suffix"

  private def dropAll(): Unit = spark.sql(s"DROP TABLE IF EXISTS $salesTbl")

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(s"""CREATE TABLE $salesTbl (
      country STRING, branch STRING, product STRING, amount DECIMAL(18,2), day TIMESTAMP
    ) USING parquet""")
    spark.sql(s"""INSERT INTO $salesTbl VALUES
      ('ES','0182','P1',100.50, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0182','P1',200.25, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0182','P2', 10.00, TIMESTAMP'2026-01-19 00:00:00'),
      ('ES','0227','P9', 33.33, TIMESTAMP'2026-01-19 00:00:00')""")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  "a typed ETL" - {

    "refuses a typed lambda, and there are two ways round it" in {
      val session = spark
      import session.implicits._
      val sales = TypedEtl.sales(spark, salesTbl)

      // WHAT FAILS. `map` does not travel as an expression: Connect serialises
      // the closure into a `UdfPacket`, uploads the compiled class as an
      // artifact and expects the server to deserialise and run it. A JVM server
      // can; Sail is Rust and cannot.
      //
      // The message is pinned on purpose, and pinned to a *bad* one. Sail
      // answers `wildcard with plan ID`, which names neither the lambda nor the
      // reason. In Sail's resolver, `resolve_map_partitions` resolves the UDF's
      // arguments before it looks at what kind of UDF it is, and a typed `map`
      // passes its arguments as a wildcard with a plan id — so the accurate
      // message Sail already has, `Scala UDF is not supported yet`, is never
      // reached. Pinning it makes this the tripwire: the day that improves, or
      // the day Sail runs the lambda outright, this goes red and says so.
      failsOnSail("wildcard with plan ID")(sales.map(_.amount * 2).collect())

      // WAY ROUND IT (1): the column, written by hand. The cast is not
      // decoration — multiplying widens the decimal to (38,18), and the JVM
      // client's Arrow reader refuses that one with `Reading
      // 'DecimalType(38,18)' values ... is not supported`.
      val byHand = sales.select((col("amount") * 2).cast("decimal(18,2)").as("doubled"))

      // WAY ROUND IT (2): the same expression through the compile-time macro,
      // which keeps the lambda's spelling and still travels as a column.
      //
      // What this does **not** do is worth stating: the `map` above still
      // fails, unchanged. This is a different call site, not a fix for that
      // one — nothing here alters what Sail is able to execute.
      val byMacro = sales.select(Expr.of[Sale](_.amount * 2).cast("decimal(18,2)").as("doubled"))

      // Three spellings, one answer, and the two that work agree exactly.
      byHand.as[BigDecimal].collect().sum shouldBe BigDecimal("688.16")
      byMacro.as[BigDecimal].collect().sum shouldBe BigDecimal("688.16")
      byMacro.collect().toSeq shouldBe byHand.collect().toSeq
    }
  }
}
