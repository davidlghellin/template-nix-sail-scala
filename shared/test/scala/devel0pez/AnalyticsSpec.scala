package devel0pez

import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{functions => F}

/** The analytics half of a real workload: explode, pivot, window functions and a temp view queried
  * with SQL.
  *
  * The ETL specs cover moving data through a pipeline. This covers what people actually do
  * afterwards, and it is where an engine is most likely to differ: window frames, approximate
  * aggregates, and arrays that have to survive the round trip as `Seq[String]`.
  *
  * Written entirely with columns, for the reason spelled out in `TypedEtl`.
  */
final class AnalyticsSpec extends SparkSuite {

  private def at(s: String) = Timestamp.valueOf(s)

  private def users = {
    val session = spark
    import session.implicits._
    Seq(
      User(1, "ES", "pro"),
      User(2, "ES", "free"),
      User(3, "US", "pro"),
      User(4, "FR", "free")
    ).toDS()
  }

  private def events = {
    val session = spark
    import session.implicits._
    Seq(
      Event(1, at("2026-01-12 09:00:00"), "click", 0.0, Seq("home", "cta")),
      Event(1, at("2026-01-12 09:01:00"), "purchase", 9.99, Seq("checkout", "promo")),
      Event(1, at("2026-01-12 09:03:00"), "purchase", 3.50, Seq("checkout")),
      Event(2, at("2026-01-12 10:00:00"), "click", 0.0, Seq("home")),
      Event(2, at("2026-01-12 10:02:00"), "purchase", 1.25, Seq("checkout")),
      Event(3, at("2026-01-12 11:00:00"), "click", 0.0, Seq("pricing", "cta")),
      Event(3, at("2026-01-12 11:04:00"), "purchase", 20.0, Seq("checkout", "vip")),
      Event(4, at("2026-01-12 12:00:00"), "click", 0.0, Seq("home", "seo"))
    ).toDS()
  }

  /** Events joined to users, with the array exploded one row per tag. */
  private def enriched =
    events
      .join(users, Seq("userId"), "left")
      // One projection rather than three chained `withColumn`s — see
      // `DataFrames.addColumns` for why that matters. A generator is allowed
      // here as long as there is only one per `select`.
      .select(
        F.col("*"),
        (F.col("eventType") === F.lit("purchase")).as("isPurchase"),
        F.to_date(F.col("ts")).as("day"),
        F.explode_outer(F.col("tags")).as("tag")
      )

  "a client-built Dataset with an array column" - {

    "round-trips as a case class" in {
      val first = events.orderBy("ts").collect().head

      first.userId shouldBe 1L
      // The array is the interesting bit: it has to come back as a Seq, not as
      // an opaque object the encoder cannot place.
      first.tags shouldBe Seq("home", "cta")
    }
  }

  "explode and aggregate" - {

    "one row per tag, and the join keeps every event" in {
      // 8 events, tags summing to 13, and every event matches a user.
      enriched.count() shouldBe 13
    }

    "daily metrics per country and segment" in {
      val daily = enriched
        .groupBy("country", "segment")
        .agg(
          F.sum(F.when(F.col("isPurchase"), F.col("amount")).otherwise(F.lit(0.0))).as("revenue"),
          F.countDistinct("userId").as("users")
        )
        .collect()
        .map(r => r.getAs[String]("country") -> r)
        .toMap

      // ES revenue is counted per exploded row, so it is the tag-weighted sum:
      // that is what makes explode-then-aggregate a trap worth having a test for.
      daily("US").getAs[Long]("users") shouldBe 1L
      daily.keySet shouldBe Set("ES", "US", "FR")
    }
  }

  "window functions" - {

    "rank purchases per user and accumulate revenue" in {
      val byUserTime = Window.partitionBy("userId").orderBy(F.col("ts").asc)

      val purchases = events
        .filter(F.col("eventType") === "purchase")
        .select(
          F.col("*"),
          F.row_number().over(byUserTime).as("rank"),
          F.sum("amount").over(byUserTime).as("running")
        )
        .orderBy("userId", "ts")
        .collect()

      val user1 = purchases.filter(_.getAs[Long]("userId") == 1L)
      user1.map(_.getAs[Int]("rank")).toSeq shouldBe Seq(1, 2)
      // 9.99 then 9.99 + 3.50
      user1.last.getAs[Double]("running") shouldBe 13.49 +- 0.001
    }

    "a moving average over a bounded frame" in {
      val frame = Window.partitionBy("userId").orderBy(F.col("ts").asc).rowsBetween(-2, 0)

      val avg = events
        .filter(F.col("eventType") === "purchase")
        .select(F.col("*"), F.avg("amount").over(frame).as("movingAvg"))
        .orderBy("userId", "ts")
        .collect()
        .filter(_.getAs[Long]("userId") == 1L)

      avg.head.getAs[Double]("movingAvg") shouldBe 9.99 +- 0.001
      avg.last.getAs[Double]("movingAvg") shouldBe 6.745 +- 0.001
    }
  }

  "a temp view queried with SQL" - {

    "ranks users by revenue" in {
      enriched.createOrReplaceTempView("events_enriched")

      val top = spark
        .sql("""
          SELECT userId, SUM(CASE WHEN eventType = 'purchase' THEN amount ELSE 0.0 END) AS revenue
          FROM events_enriched
          GROUP BY userId
          ORDER BY revenue DESC
          LIMIT 1
        """)
        .collect()
        .head

      // User 3 spent 20.0, the most of anyone.
      top.getAs[Long]("userId") shouldBe 3L
    }
  }
}
