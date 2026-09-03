package devel0pez

import java.sql.{Date, Timestamp}

/** The staged ETL: four `Dataset[A] => Dataset[B]` values, applied with `transform` and composed
  * with `andThen`.
  *
  * The landing table is all Strings and carries three rows that cannot survive parsing or
  * validation, because a pipeline that is only ever shown the happy path proves nothing about the
  * branch that matters at 3am.
  */
final class PipelineEtlSpec extends SparkSuite {

  private val suffix = backend.replace('-', '_')
  private val rawTbl = s"sales_raw_$suffix"
  private val prodTbl = s"products_pipe_$suffix"

  private val audited = Timestamp.valueOf("2026-01-20 15:04:31")
  private val day = Date.valueOf("2026-01-20")

  private def dropAll(): Unit =
    Seq(rawTbl, prodTbl).foreach(t => spark.sql(s"DROP TABLE IF EXISTS $t"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    dropAll()
    // Every column a String: this is a landing zone, not a model.
    spark.sql(s"""CREATE TABLE $rawTbl (
      country STRING, branch STRING, product STRING, amount STRING, day STRING
    ) USING parquet""")
    spark.sql(s"""CREATE TABLE $prodTbl (
      code STRING, name STRING, family STRING
    ) USING parquet""")

    spark.sql(s"""INSERT INTO $rawTbl VALUES
      ('ES','0182','P1','100.50','2026-01-19 00:00:00'),
      ('ES','0182','P1','200.25','2026-01-19 00:00:00'),
      ('ES','0182','P2',' 10.00 ','2026-01-19 00:00:00'),
      ('ES','0227','P9','33.33','2026-01-19 00:00:00'),
      ('ES','0182','P1','not-a-number','2026-01-19 00:00:00'),
      ('ES','0182','P1','5.00','yesterday'),
      ('ES','','P1','7.00','2026-01-19 00:00:00')""")
    spark.sql(s"""INSERT INTO $prodTbl VALUES
      ('P1','Widget','TOOLS'), ('P2','Gadget','TOOLS')""")
  }

  override def afterAll(): Unit =
    try dropAll()
    finally super.afterAll()

  private def raw = PipelineEtl.read(spark, rawTbl)

  private def products = {
    val session = spark
    import session.implicits._
    spark.table(prodTbl).as[Product]
  }

  "the staged pipeline" - {

    "reads the landing table as Strings" in {
      raw.schema.fields.map(_.dataType.typeName).toSet shouldBe Set("string")
      raw.count() shouldBe 7
    }

    "parses with try_cast, so a bad row becomes NULL instead of an error" in {
      val parsed = raw.transform(PipelineEtl.parse).collect()

      // The whole point of try_cast over cast: with ANSI on, `cast` would have
      // raised here and taken the other six rows with it.
      parsed.count(_.amount == null) shouldBe 1
      parsed.count(_.day == null) shouldBe 1
      // And the good ones really did get types, whitespace included.
      parsed.flatMap(s => Option(s.amount)).max shouldBe BigDecimal("200.25")
      parsed.count(_.amount == BigDecimal("10.00")) shouldBe 1
    }

    "splits validated rows from rejected ones, with no row in both or neither" in {
      val parsed = raw.transform(PipelineEtl.parse)

      val valid = parsed.transform(PipelineEtl.validate)
      val bad = parsed.transform(PipelineEtl.rejected)

      valid.count() shouldBe 4
      bad.count() shouldBe 3
      // The two branches come from one predicate and its negation, so this
      // has to hold — and if someone edits one branch only, it stops holding.
      valid.count() + bad.count() shouldBe parsed.count()
    }

    "says why each row was rejected" in {
      val reasons =
        raw
          .transform(PipelineEtl.parse)
          .transform(PipelineEtl.rejected)
          .collect()
          .map(_.reason)
          .toSet

      reasons shouldBe Set(
        "amount is not a number",
        "day is not a timestamp",
        "a key field is empty"
      )
    }

    "aggregates what survived" in {
      val result =
        raw
          .transform(PipelineEtl.pipeline(products, audited, day))
          .collect()
          .map(r => (r.branch, r.family) -> r)
          .toMap

      // 100.50 + 200.25 + 10.00, all TOOLS at branch 0182.
      result(("0182", "TOOLS")).total shouldBe BigDecimal("310.75")
      // P9 is not in the catalogue and still comes out.
      result(("0227", "UNKNOWN")).total shouldBe BigDecimal("33.33")
      result(("0182", "TOOLS")).audited shouldBe audited
      result(("0182", "TOOLS")).day shouldBe day
    }

    "composing with andThen is the same as chaining with transform" in {
      // `ds.transform(f).transform(g) == ds.transform(f andThen g)`. This is
      // the identity the whole design rests on, so it is asserted rather than
      // claimed in a comment.
      val chained = raw
        .transform(PipelineEtl.parse)
        .transform(PipelineEtl.validate)
        .transform(PipelineEtl.enrich(products))
        .transform(PipelineEtl.byFamily(audited, day))

      val composed = raw.transform(PipelineEtl.pipeline(products, audited, day))

      chained.schema shouldBe composed.schema
      chained.collect().toSeq should contain theSameElementsAs composed.collect().toSeq
    }

    "a stage is a plain function, testable without the rest of the pipeline" in {
      val session = spark
      import session.implicits._

      // No landing table, no catalogue, no aggregation: just the one stage,
      // fed a Dataset built in the test. That is what makes a stage a value
      // rather than a step buried in a method.
      val one = Seq(ValidSale("ES", "0182", "P1", BigDecimal("1.00"), audited)).toDS()

      val enriched = one.transform(PipelineEtl.enrich(products)).collect()

      enriched.head.family shouldBe "TOOLS"
    }
  }
}
