package devel0pez

/** Decimal type promotion, where the two engines do not yet agree.
  *
  * This started as a probe that printed types and asserted nothing. It asserts now, because the
  * divergence it prints looks like a gap in Sail rather than a decision — Sail is under active
  * development, and these rules are the sort of thing implemented last. Pinning both sides makes
  * this a tripwire: when Sail's promotion matches Catalyst's, the stale arm fails and says so.
  *
  * The diagnosis, as far as the numbers go: they agree on everything except **how wide an integer
  * literal is**. Multiplying `DECIMAL(18,2)` by an explicit `DECIMAL(1,0)` gives `(20,2)` on both;
  * by an explicit `DECIMAL(10,0)` gives `(29,2)` on both. It is only the bare literal `2` that
  * parts them — Catalyst narrows it to its smallest decimal, Sail keeps it at an `Int`'s width.
  *
  * The values are equal either way. It is the schema that differs, which is invisible until the
  * result meets a table.
  */
final class DecProbe extends SparkSuite {

  private def typeOf(sql: String) = spark.sql(sql).schema.head.dataType.simpleString

  "decimal promotion" - {

    "agrees when both operands say their precision" in {
      typeOf("SELECT CAST(1.5 AS DECIMAL(18,2)) * CAST(2 AS DECIMAL(1,0)) AS r") shouldBe
        "decimal(20,2)"
      typeOf("SELECT CAST(1.5 AS DECIMAL(18,2)) * CAST(2 AS DECIMAL(10,0)) AS r") shouldBe
        "decimal(29,2)"
      typeOf("SELECT CAST(1.5 AS DECIMAL(18,2)) + CAST(1.5 AS DECIMAL(18,2)) AS r") shouldBe
        "decimal(19,2)"
    }

    "and parts company over the width of a bare literal" in {
      // The literal itself is an Int on both, so the disagreement is in what
      // that Int becomes when it meets a decimal.
      typeOf("SELECT 2 AS r") shouldBe "int"

      perEngine {
        // Catalyst narrows `2` to decimal(1,0) — the smallest that holds it.
        typeOf("SELECT CAST(1.5 AS DECIMAL(18,2)) * 2 AS r") shouldBe "decimal(20,2)"
      } {
        // Sail keeps an Int's full width, as if it had been decimal(10,0).
        typeOf("SELECT CAST(1.5 AS DECIMAL(18,2)) * 2 AS r") shouldBe "decimal(29,2)"
      }
    }

    "and over how it gives ground when precision would overflow" in {
      // `amount` is decimal(38,18): there is no room to widen, so something has
      // to be sacrificed, and the two sacrifice differently. This is the one an
      // ETL notices, because 38 is where real money columns already sit.
      val amount = "CAST(1.5 AS DECIMAL(38,18))"

      perEngine {
        typeOf(s"SELECT $amount * 2 AS r") shouldBe "decimal(38,16)"
        typeOf(s"SELECT $amount + $amount AS r") shouldBe "decimal(38,17)"
        typeOf(s"SELECT $amount / 2 AS r") shouldBe "decimal(38,18)"
      } {
        typeOf(s"SELECT $amount * 2 AS r") shouldBe "decimal(38,18)"
        typeOf(s"SELECT $amount + $amount AS r") shouldBe "decimal(38,18)"
        typeOf(s"SELECT $amount / 2 AS r") shouldBe "decimal(38,22)"
      }
    }
  }
}
