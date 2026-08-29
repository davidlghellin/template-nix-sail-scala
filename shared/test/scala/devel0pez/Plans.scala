package devel0pez

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.spark.sql.Dataset

/** Reading a query plan from a test, on either engine.
  *
  * `Dataset.explain` returns `Unit` and prints, so the only portable way to look at a plan is to
  * catch what it printed. `queryExecution`, which would hand back the plan as an object, exists on
  * classic and throws `UNSUPPORTED_CONNECT_FEATURE.DATASET_QUERY_EXECUTION` over Connect — so it is
  * the classic-only door, not the general one.
  *
  * Both `System.out` and Scala's `Console` are redirected: the classic client prints through
  * Scala's `println`, and there is no guarantee the Connect client does the same. Tests run
  * single-threaded here (`Test / parallelExecution := false`), which is what makes swapping a
  * process-wide stream safe.
  */
object Plans {

  /** The physical plan, as printed. */
  def of(ds: Dataset[_]): String = capture(ds.explain())

  /** The plan in one of `explain`'s modes: `simple`, `extended`, `formatted`, `cost`. */
  def of(ds: Dataset[_], mode: String): String = capture(ds.explain(mode))

  /** How many times a node name appears — a crude but honest measure of plan shape. */
  def count(plan: String, node: String): Int = node.r.findAllIn(plan).size

  private def capture(body: => Unit): String = {
    // The `PrintStream` below is deliberately not closed. It wraps a
    // `ByteArrayOutputStream`, whose own `close` is documented as having no
    // effect, so there is no descriptor and nothing to release — and it is
    // built with `autoFlush = true`, so there is nothing pending either.
    // What does need care is `System.setOut`, which is process-wide; the
    // `finally` restores it, and the suite runs single-threaded.
    val buffer = new ByteArrayOutputStream()
    val previous = System.out
    try {
      System.setOut(new PrintStream(buffer, true))
      Console.withOut(buffer)(body)
    } finally System.setOut(previous)
    buffer.toString
  }
}
