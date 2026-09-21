package devel0pez

import Conform._

/** The `Conform` typeclass, and the silent failure it exists to prevent.
  *
  * The first test is the one that matters: it shows a `Dataset[Product]` that passes every
  * assertion you would think to write and still writes the wrong values into a table. Everything
  * else here is the shape of the fix.
  */
final class ConformSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val naiveTbl = s"catalogue_naive_$suffix"
  private val conformedTbl = s"catalogue_conformed_$suffix"

  private def dropAll(): Unit =
    Seq(naiveTbl, conformedTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    Seq(naiveTbl, conformedTbl).foreach { t =>
      spark.sql(s"CREATE TABLE $t (code STRING, name STRING, family STRING) USING parquet")
    }
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  /** The catalogue row, with the columns in the wrong order — as a careless `select` leaves it. */
  private def reversed = {
    val session = spark
    import session.implicits._
    Seq(("TOOLS", "Widget", "P1")).toDF("family", "name", "code")
  }

  "as[T] on its own" - {

    "decodes by name, so every assertion you would write passes" in {
      val session = spark
      import session.implicits._

      reversed.as[Product].collect().head shouldBe Product("P1", "Widget", "TOOLS")
    }

    "but leaves the schema in the source order" in {
      val session = spark
      import session.implicits._

      // This is the whole problem in one line: correct values, wrong shape.
      reversed.as[Product].schema.fieldNames.toSeq shouldBe Seq("family", "name", "code")
    }

    "so insertInto, which matches by position, writes the wrong values" in {
      val session = spark
      import session.implicits._

      reversed.as[Product].write.insertInto(naiveTbl)

      // Nothing raised. The table now holds the family where the code belongs,
      // and the only reason this test is green is that it asserts the damage.
      //
      // This is also the one warning a clean run still prints:
      // `TableOutputResolver: The query columns and the table columns have same
      // names but different orders`. Spark is right, and it is describing this
      // very line. The logger is left on deliberately — see the note in
      // `log4j2.properties`.
      spark.table(naiveTbl).as[Product].collect().head shouldBe Product("TOOLS", "Widget", "P1")
    }
  }

  "conformTo" - {

    "puts the columns in the order the case class declares" in {
      reversed.conformTo[Product].schema.fieldNames.toSeq shouldBe Seq("code", "name", "family")
    }

    "so the same row reaches the table intact" in {
      val session = spark
      import session.implicits._

      reversed.conformTo[Product].write.insertInto(conformedTbl)

      spark.table(conformedTbl).as[Product].collect().head shouldBe Product("P1", "Widget", "TOOLS")
    }

    "projects away a column the case class does not declare" in {
      val session = spark
      import session.implicits._
      val wide = Seq(("P1", "Widget", "TOOLS", "ignore me")).toDF("code", "name", "family", "junk")

      wide.conformTo[Product].schema.fieldNames.toSeq shouldBe Seq("code", "name", "family")
    }

    "fails on a missing column, and names it" in {
      val session = spark
      import session.implicits._
      val short = Seq(("P1", "Widget")).toDF("code", "name")

      val error = intercept[ConformError](short.conformTo[Product])

      error.getMessage should include("missing columns: family")
    }

    "is what stops the two engines from disagreeing about a missing column" in {
      val session = spark
      import session.implicits._
      val short = Seq(("P1", "Widget")).toDF("code", "name")

      // `to` on its own is not a contract: the engines do not even agree on
      // what it means. Classic Spark invents the column and fills it with
      // nulls — a schema derived from a case class has every field nullable,
      // so that path is always open. Sail refuses the plan outright.
      perEngine {
        short.to(Conform[Product].schema).collect().head.isNullAt(2) shouldBe true
      } {
        val refused = intercept[Exception](short.to(Conform[Product].schema).collect())
        refused.getMessage should include("field not found in input schema: family")
      }

      // The guard runs before `to`, so both engines give the same answer —
      // which is the point of the typeclass, and of this template.
      intercept[ConformError](short.conformTo[Product]).getMessage should
        include("missing columns: family")
    }
  }

  "Conform.exact" - {

    "refuses the extra column the default instance would drop" in {
      val session = spark
      import session.implicits._
      val wide = Seq(("P1", "Widget", "TOOLS", "surprise")).toDF("code", "name", "family", "junk")

      val error = intercept[ConformError](wide.conformTo[Product](Conform.exact))

      error.getMessage should include("unexpected columns: junk")
    }

    "still accepts a frame that matches exactly, whatever the order" in {
      reversed.conformTo[Product](Conform.exact).collect().head shouldBe
        Product("P1", "Widget", "TOOLS")
    }
  }

  "the instance" - {

    "is summonable without a session" in {
      // No `spark` here on purpose: derived from `Encoders.product[T]`, which
      // needs a TypeTag and nothing else.
      Conform[Sale].schema.fieldNames.toSeq shouldBe
        Seq("country", "branch", "product", "amount", "day")
    }
  }
}
