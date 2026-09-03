package devel0pez

import java.sql.Date

/** Where the two engines disagree about writing to a partitioned table.
  *
  * Split out of `PartitionedSpec`, which keeps the part that is advice about how to write a job.
  * This half is a fact about Sail as it is today, and the second block is the most expensive
  * divergence in this repository: it does not raise, it succeeds and deletes data.
  *
  * `Daily` is declared in `PartitionedSpec`.
  */
final class PartitionedDivergenceSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val declaredFirst = s"div_first_$suffix"
  private val table = s"div_daily_$suffix"

  private def dropAll(): Unit =
    Seq(declaredFirst, table).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    spark.sql(
      s"CREATE TABLE $declaredFirst (day DATE, k STRING, v INT) USING parquet PARTITIONED BY (day)"
    )
    seed()
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def rows(t: String): Seq[String] =
    spark.table(t).collect().map(_.toString).sorted.toSeq

  private def seed(): Unit = {
    spark.sql(s"DROP TABLE IF EXISTS $table")
    spark.sql(s"CREATE TABLE $table (k STRING, v INT, day DATE) USING parquet PARTITIONED BY (day)")
    spark.sql(s"INSERT INTO $table VALUES ('a',1,DATE'2026-01-19'), ('b',2,DATE'2026-01-20')")
  }

  "values supplied in the order you declared" - {

    "are refused by classic and accepted by Sail" in {
      // `INSERT INTO t VALUES (DATE'...', 'a', 1)` — the declaration order, not
      // the reported one. The engines take opposite views, and neither is
      // obviously wrong; what matters is that they differ.
      perEngine {
        // Classic matches positionally against the reordered schema, so the date
        // lands on `k STRING`, and ANSI refuses the cast. The error is ugly and
        // it is doing its job.
        val refused = intercept[Exception](
          spark.sql(s"INSERT INTO $declaredFirst VALUES (DATE'2026-01-19','a',1)")
        )
        refused.getMessage should include("INCOMPATIBLE_DATA_FOR_TABLE")
      } {
        // Sail keeps the declared order and writes the row the author meant.
        // Friendlier in isolation, and a portability hazard in a project that
        // runs on both: this statement is a job on one engine and an error on
        // the other.
        spark.sql(s"INSERT INTO $declaredFirst VALUES (DATE'2026-01-19','a',1)")
        rows(declaredFirst) shouldBe Seq("[a,1,2026-01-19]")
      }
    }
  }

  "reprocessing one day" - {

    "is what partitionOverwriteMode=dynamic is for, and both engines accept the setting" in {
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
      spark.conf.get("spark.sql.sources.partitionOverwriteMode") shouldBe "dynamic"
    }

    "keeps the other partitions on classic and deletes them on Sail" in {
      import Conform._
      val session = spark
      import session.implicits._
      seed()
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

      // Re-run one day, the way a daily job reruns yesterday.
      Seq(Daily("a2", 99, Date.valueOf("2026-01-19")))
        .toDS()
        .conformTo[Daily]
        .write
        .mode("overwrite")
        .insertInto(table)

      perEngine {
        // Only the 19th was replaced. This is the whole point of `dynamic`.
        rows(table) shouldBe Seq("[a2,99,2026-01-19]", "[b,2,2026-01-20]")
      } {
        // Sail reports the config back and then does a static overwrite: the
        // 20th is gone. Nothing raised, and a job that reruns yesterday every
        // morning would truncate the table every morning.
        rows(table) shouldBe Seq("[a2,99,2026-01-19]")
      }
    }

    "and static overwrite means the same thing on both, which is the safe fallback" in {
      import Conform._
      val session = spark
      import session.implicits._
      seed()
      spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")

      Seq(Daily("a2", 99, Date.valueOf("2026-01-19")))
        .toDS()
        .conformTo[Daily]
        .write
        .mode("overwrite")
        .insertInto(table)

      // Both wipe the table and leave one row. Agreeing on destruction is still
      // agreeing: a job written this way behaves the same on either engine.
      rows(table) shouldBe Seq("[a2,99,2026-01-19]")
    }
  }
}
