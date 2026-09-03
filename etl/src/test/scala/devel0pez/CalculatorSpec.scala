package devel0pez

import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers

/** Does not extend SparkSuite: it needs no session, so it runs instantly. */
final class CalculatorSpec extends AnyFreeSpec with Matchers {

  "Calculator.add" - {
    "adds two positives" in {
      Calculator.add(2, 3) shouldBe 5
    }

    "adding zero leaves the number unchanged" in {
      Calculator.add(7, 0) shouldBe 7
    }

    "adds negatives" in {
      Calculator.add(-2, -3) shouldBe -5
    }
  }
}
