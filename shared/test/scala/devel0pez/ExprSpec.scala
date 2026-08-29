package devel0pez

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.DecimalType

import devel0pez.macros.Expr
import devel0pez.macros.Expr._
import devel0pez.macros.TypedDataset

/** The macro spike: does translating a lambda at compile time actually buy anything?
  *
  * Three questions, in order of how much they matter.
  *
  * Does it produce the *same answer* as the column written by hand — on both engines, not just on
  * classic? A translator that is nearly right is worse than no translator.
  *
  * Does an unsupported lambda fail at **compile** time? That is the property the whole design rests
  * on. Run-time failure would be no better than what Sail already does.
  *
  * And, the one the spike exists to settle: is the result worth writing down, given that the column
  * handles already got the strings out of the way?
  */
/** What a `map` to a case class produces — the canonical shape of a typed ETL step. */
final case class Doubled(country: String, doubled: BigDecimal)

/** Returns a `Doubled` without constructing one in place: the shape the macro must refuse. */
object notAConstructor {
  def apply(country: String, amount: BigDecimal): Doubled = Doubled(country, amount)
}

final class ExprSpec extends SparkSuite {

  private def sales = {
    val session = spark
    import session.implicits._
    Seq(
      Sale("ES", "0182", "P1", BigDecimal("100.50"), BaseCase.Cutoff),
      Sale("ES", "0227", "P9", BigDecimal("33.33"), BaseCase.Cutoff)
    ).toDS()
  }

  "the macro" - {

    "translates a field read into the column of the same name" in {
      val doubled = Expr.of[Sale](_.amount * 2).cast(DecimalType(18, 2)).as("doubled")

      // `getAs[BigDecimal]` would hand back a java.math.BigDecimal; go through the
      // encoder instead, which is the typed path this template uses everywhere.
      val session = spark
      import session.implicits._
      sales.select(doubled).as[BigDecimal].collect().sum shouldBe BigDecimal("267.66")
    }

    "gives exactly what the hand-written column gives" in {
      // The claim that matters. If these ever diverge the macro is a liability,
      // so it is asserted rather than assumed.
      val byMacro = sales.select(Expr.of[Sale](_.amount * 2).cast(DecimalType(18, 2)).as("v"))
      val byHand = sales.select((col("amount") * lit(2)).cast(DecimalType(18, 2)).as("v"))

      byMacro.schema shouldBe byHand.schema
      byMacro.collect().toSeq shouldBe byHand.collect().toSeq
    }

    "carries comparisons as well as arithmetic" in {
      val big = sales.filter(Expr.of[Sale](_.amount > 50)).collect()

      big.map(_.product).toSeq shouldBe Seq("P1")
    }

    "runs on both engines, because nothing is ever shipped as a closure" in {
      // The point of the exercise: `sales.map(_.amount * 2)` fails on Sail —
      // `TypedEtlSpec` pins that. The same expression through the macro is a
      // plain Column, so it travels in the plan like any other.
      sales.select(Expr.of[Sale](_.amount * 2).cast(DecimalType(18, 2))).count() shouldBe 2
    }
  }

  "the Dataset shapes that fail on Sail" - {

    // Measured: on Sail, `filter(lambda)`, `map(lambda)`, `flatMap` and `groupByKey` all fail.
    // The first three die on `wildcard with plan ID`; `groupByKey` reaches the accurate
    // `Scala UDF is not supported yet`. These two cover the ones an ETL reaches for most.

    "filterExpr stands in for a typed filter, on both engines" in {
      val kept = sales.filterExpr(_.amount > 50).collect().map(_.product).toSeq

      kept shouldBe Seq("P1")
    }

    "mapExpr builds a case class, one column per field" in {
      val session = spark
      import session.implicits._
      val out = sales.mapExpr(s => Doubled(s.country, s.amount * 2))

      // The field names come from the case class, in declaration order, which is
      // what `insertInto` will later match by position.
      out.schema.fieldNames.toSeq shouldBe Seq("country", "doubled")
      out.collect().map(_.country).toSeq shouldBe Seq("ES", "ES")
    }

    "and the two chain, like the lambdas they replace" in {
      val session = spark
      import session.implicits._
      val out = sales
        .filterExpr(_.amount > 50)
        .mapExpr(s => Doubled(s.country, s.amount * 2))
        .collect()

      out.length shouldBe 1
      out.head.country shouldBe "ES"
    }
  }

  "the normal spelling, on a type of our own" - {

    // `Dataset.map` cannot be intercepted — a member always beats an implicit
    // conversion. Wrapping is the way round it: on `TypedDataset`, `map` and
    // `filter` are names like any other, so the call site reads exactly like the
    // code that fails on Sail while compiling to a projection instead.

    "ds.map reads the same and runs on both engines" in {
      val session = spark
      import session.implicits._
      val ds = TypedDataset(sales)

      ds.map(_.amount * 2).dataset.count() shouldBe 2
    }

    "ds.filter too" in {
      val ds = TypedDataset(sales)

      ds.filter(_.amount > 50).dataset.collect().map(_.product).toSeq shouldBe Seq("P1")
    }

    "and they chain into a case class, which is the shape ETLs are written in" in {
      val session = spark
      import session.implicits._
      val ds = TypedDataset(sales)

      val out = ds.filter(_.amount > 50).map(s => Doubled(s.country, s.amount * 2)).dataset

      out.schema.fieldNames.toSeq shouldBe Seq("country", "doubled")
      out.collect().map(_.country).toSeq shouldBe Seq("ES")
    }
  }

  "an unsupported lambda" - {

    "does not compile, rather than failing at run time" in {
      // A method call is outside the subset. This is the failure mode the design
      // is chosen for: the compiler stops it, so it cannot become a plausible
      // wrong answer against one engine and a right one against the other.
      // A method call on a field: outside the subset.
      assertDoesNotCompile("""Expr.of[Sale](s => s.product.toUpperCase)""")
      // Reads no field, but is not a literal either. Deterministic on purpose:
      // the property under test is "not a literal", and a random value would
      // muddle that with a question about determinism. Lifting this would
      // silently turn a per-row expression into a driver-side constant.
      assertDoesNotCompile("""Expr.of[Sale](_ => "abc".length)""")
    }

    "only splits a lambda that actually constructs the target" in {
      val session = spark
      import session.implicits._

      // An earlier version matched any call whose arity equalled the field
      // count, so `s => swapped(s.a, s.b)` compiled as `Doubled(s.a, s.b)` —
      // ignoring what `swapped` did and answering something else. Now the
      // constructor is checked, and anything else is refused.
      assertCompiles("""sales.mapExpr(s => Doubled(s.country, s.amount * 2))""")
      assertDoesNotCompile("""sales.mapExpr(s => notAConstructor(s.country, s.amount))""")
    }

    "allows the integer operators that both engines agree on" in {
      // Measured: `5 % 2` is 1 on both, and `5 / 2.0` is 2.5 on both. Only
      // integer-by-integer division parts them, so only that is refused.
      assertCompiles("""Expr.of[Event](_.userId % 2)""")
      assertCompiles("""Expr.of[Event](_.userId / 2.0)""")
      assertDoesNotCompile("""Expr.of[Event](_.userId / 2)""")
    }

    "is refused when the operator would mean something else" in {
      // Measured: Scala's `5 / 2` is 2, Spark's `col / 2` is 2.5 with type
      // Double. Translating it would compile, run, and answer differently —
      // so it is rejected instead.
      assertDoesNotCompile("""Expr.of[Event](_.userId / 2)""")
      // `+` concatenates Strings in Scala and is arithmetic in Spark.
      assertDoesNotCompile("""Expr.of[Sale](s => s.country + s.branch)""")
    }

    "translates == to the null-safe comparison, not to ===" in {
      val session = spark
      import session.implicits._
      // `===` answers NULL when either side is NULL; Scala's `==` answers
      // false. `<=>` is the one that agrees with the lambda, and the
      // difference shows up on a nullable column.
      val readings = Seq(
        Reading("m1", BaseCase.Audit, BigDecimal("1.0"), Some("NOCTURNA")),
        Reading("m2", BaseCase.Audit, BigDecimal("1.0"), None)
      ).toDS()

      readings.filter(Expr.of[Reading](_.tariff == "NOCTURNA")).count() shouldBe 1L
      // And the row with NULL is genuinely excluded rather than erroring.
      readings.filter(Expr.of[Reading](_.tariff != "NOCTURNA")).count() shouldBe 1L
    }

    "while the supported subset does compile" in {
      assertCompiles("""Expr.of[Sale](_.amount * 2)""")
      assertCompiles("""Expr.of[Sale](_.amount > 10)""")
    }
  }
}
