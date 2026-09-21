package devel0pez

import cats.data.{NonEmptyList, Validated}
import cats.implicits._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.{col, sum}

/** Cats over Spark, and where each layer of the abstraction stops working.
  *
  * The question worth asking is not "does Cats run on Sail" — it is Scala, of course it does. It is
  * whether the abstractions people reach for when they bring Cats to Spark survive, and the answer
  * comes in three layers that fail for three different reasons. Only the last one is about Sail.
  */
final class CatsSpec extends SparkSuite {

  private def sales = {
    val session = spark
    import session.implicits._
    Seq(
      Sale("ES", "0182", "P1", BigDecimal("100.50"), BaseCase.Cutoff),
      Sale("ES", "0227", "P9", BigDecimal("33.33"), BaseCase.Cutoff)
    ).toDS()
  }

  "layer 1: the abstraction does not fit the type, on either engine" - {

    "there is no Functor[Dataset], and Spark is not to blame" in {
      // `Functor[F[_]]` promises `map` for **every** `B`. `Dataset.map` demands
      // an `Encoder[B]`, and the signature of `Functor.map` has nowhere to put
      // one. This is the constrained-monad problem, it is a fact about Scala's
      // type classes rather than about Spark, and it bites identically on
      // classic. Nothing here has reached an engine yet.
      assertDoesNotCompile("""
        new cats.Functor[org.apache.spark.sql.Dataset] {
          def map[A, B](fa: org.apache.spark.sql.Dataset[A])(f: A => B): org.apache.spark.sql.Dataset[B] =
            fa.map(f)
        }
      """)
    }

    "while the same body compiles the moment an Encoder is in scope" in {
      // The control, so the assertion above is failing for the reason claimed
      // rather than because the snippet was malformed. Fix `B` and the encoder
      // can be found — which is exactly what `Functor` has no way to ask for.
      //
      // Compilation only, deliberately. An earlier version of this test *ran*
      // the result, and on Sail it died with `wildcard with plan ID` — proving
      // layer 2 by accident and destroying its own purpose, which is to say
      // something about types before any engine is involved.
      assertCompiles("""
        val session = spark
        import session.implicits._
        def mapToString[A](ds: Dataset[A])(f: A => String): Dataset[String] = ds.map(f)
        mapToString(sales)(_.branch)
      """)
    }
  }

  "layer 2: where you can make it fit, it lands on the one thing Sail refuses" - {

    "a Functor at a fixed output type works on classic and not on Sail" in {
      val session = spark
      import session.implicits._

      // The usual workaround: pin `B`, capture its encoder, and you have
      // something Functor-shaped. It composes, the laws hold — and every use of
      // it is a closure, which is precisely what does not travel.
      def fmap(ds: Dataset[Sale])(f: Sale => String): Dataset[String] = ds.map(f)

      failsOnSail("wildcard with plan ID") {
        fmap(sales)(_.branch).collect().toSet shouldBe Set("0182", "0227")
      }
    }

    "and so does anything built on flatMap, which is most of the library" in {
      val session = spark
      import session.implicits._

      // Monad, traverse, foldM: all of it reduces to `map` and `flatMap`. The
      // abstraction is not partially supported here, it is supported to exactly
      // zero percent, because its two primitives are the two Sail cannot run.
      failsOnSail("wildcard with plan ID") {
        sales.flatMap(s => List(s.branch, s.product)).collect() should have size 4
      }
    }
  }

  "layer 3: Cats over values rather than over Dataset works on both" - {

    "Validated accumulates every quality problem instead of throwing on the first" in {
      // This is the shape worth stealing. `Quality.nonNullKey` throws, so a run
      // reports one problem per attempt; `ValidatedNel` reports all of them at
      // once. And it runs identically on both engines because the checks are
      // Columns — what Cats combines is the *answers*, on the driver, not the
      // rows.
      type Checked[A] = Validated[NonEmptyList[String], A]

      def notEmpty(ds: Dataset[Sale]): Checked[Long] = {
        val n = ds.count()
        if (n > 0) n.validNel else "the dataset is empty".invalidNel
      }

      def noNullBranch(ds: Dataset[Sale]): Checked[Long] = {
        val nulls = ds.filter(col("branch").isNull).count()
        if (nulls == 0) 0L.validNel else s"branch has $nulls nulls".invalidNel
      }

      def totalIsPositive(ds: Dataset[Sale]): Checked[BigDecimal] = {
        // Off the `Row` rather than through an encoder: `Encoders.DECIMAL` is
        // Java's `BigDecimal`, which has no `>`. And `sum` over no rows is
        // NULL, not zero — the case this check exists to catch, so reading it
        // without asking would throw before it could be reported.
        val row = ds.select(sum(col("amount"))).head()
        val total = if (row.isNullAt(0)) BigDecimal(0) else BigDecimal(row.getDecimal(0))
        if (total > 0) total.validNel else "the total is not positive".invalidNel
      }

      val good = (notEmpty(sales), noNullBranch(sales), totalIsPositive(sales)).tupled
      good.isValid shouldBe true

      // And on a dataset that fails two checks at once, both are named — which
      // is the whole reason to reach for `Validated` over an exception.
      val session = spark
      import session.implicits._
      val empty = Seq.empty[Sale].toDS()
      val bad = (notEmpty(empty), totalIsPositive(empty)).tupled

      bad.isInvalid shouldBe true
      val problems = bad.fold(_.toList, _ => Nil)
      problems should have size 2
      problems.mkString(" ") should include("empty")
    }

    "and Monoid combines results the engine has already computed" in {
      val session = spark
      import session.implicits._

      // `combineAll` over collected values, not over the Dataset. Cats never
      // sees Spark, so there is nothing for an engine to refuse — and nothing
      // is gained over `sum` either, which is the honest note: this is fine for
      // stitching together results, and it is not a distributed aggregation.
      val branches = sales.select(col("branch")).as[String].collect().toList
      branches.combineAll shouldBe "01820227"

      val totals = Map("ES" -> BigDecimal(100)) |+| Map("ES" -> BigDecimal(33))
      totals("ES") shouldBe BigDecimal(133)
    }
  }
}
