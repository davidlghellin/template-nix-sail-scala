package devel0pez.etl

import java.nio.file.{Files, Path, Paths}

import scala.jdk.CollectionConverters._

import devel0pez.SparkSuite
import devel0pez.etl.jobs.{Ciudad, Ciudades, Jobs, PoblacionCcaa, PorCcaa}

/** The chain, run end to end — which is also the whole of its dry-run.
  *
  * The Python original has a `--dry-run` whose main job is to answer, without starting Spark,
  * whether the schema each job produces is the schema the next one consumes. That question does not
  * survive translation: `PorCcaa` is a `Job[Ciudad, PoblacionCcaa]` whose `consumes` **is**
  * `Ciudades.produces`, so a mismatch is a compile error and there is nothing left to check at run
  * time.
  *
  * What is left is everything a type cannot know, and it is all here:
  *
  *   - the graph properties — one producer per dataset, no cycle, the link still present
  *   - the data — a missing file, a header in the wrong order
  *   - the domain — that the numbers are right
  *
  * On both engines, because that is the point of this template.
  */
final class EtlChainSpec extends SparkSuite {

  private var root: Path = _

  /** A root with the source file in it, so the run exercises path resolution too.
    *
    * Copied rather than pointed at: `EtlConfig` prefixes every relative path with the root, inputs
    * included, which is how the same code writes to a laptop and to a bucket.
    */
  override def beforeAll(): Unit = {
    super.beforeAll()
    root = Files.createTempDirectory(s"etl-${backend.replace('-', '_')}-")
    val resources = root.resolve("resources")
    Files.createDirectories(resources)
    Files.copy(
      Paths.get("resources/ciudades_espana.csv"),
      resources.resolve("ciudades_espana.csv")
    )
  }

  override def afterAll(): Unit =
    try
      if (root != null) {
        Files
          .walk(root)
          .sorted(java.util.Comparator.reverseOrder[Path]())
          .iterator()
          .asScala
          .foreach(Files.deleteIfExists)
      }
    finally super.afterAll()

  private def config = EtlConfig("dev", root.toString)

  "the chain, derived from what the jobs declare" - {

    "runs the jobs in dependency order" in {
      Jobs.graph.order shouldBe Some(Seq("ciudades", "por_ccaa"))
    }

    "has exactly one producer per dataset" in {
      // The check that stops two jobs quietly writing the same place.
      val producers = Jobs.all.groupBy(_.produces.name).view.mapValues(_.size).toMap
      producers.filter(_._2 > 1) shouldBe empty
    }

    "still links the two jobs through the dataset they share" in {
      // If someone repoints `PorCcaa.consumes` at a fresh DataRef with the same
      // path, this is what notices: the edge disappears while everything still
      // compiles and still runs.
      Jobs.graph.edges shouldBe Seq(("ciudades", "por_ccaa", "ciudades_dedup"))
      PorCcaa.consumes should be theSameInstanceAs Ciudades.produces
    }

    "names one external input and one final output" in {
      Jobs.graph.externalInputs shouldBe Seq("ciudades_raw")
      Jobs.graph.finalOutputs shouldBe Seq("poblacion_por_ccaa")
    }

    "renders the chain as text, with the resolved paths" in {
      val text = Jobs.graph.render(config)

      // The text view is what someone reads before launching, so it has to show
      // where the data will actually come from and go — resolved against the
      // environment, not the relative path the job declares.
      text should include("ciudades_raw")
      text should include(root.toString)
      text should include("(external)")
      text should include("(from ciudades)")
      text should include("ciudades -> por_ccaa")
    }

    "and the mermaid view names the same datasets as the text one" in {
      // Both are generated from the same `consumes`/`produces`, so this is
      // guarding the rendering rather than the graph.
      val mermaid = Jobs.graph.renderMermaid
      Jobs.all.foreach { job =>
        mermaid should include(job.consumes.name)
        mermaid should include(job.produces.name)
      }
    }
  }

  "the domain, with no session state involved" - {

    "rejects a null key" in {
      val session = spark
      import session.implicits._
      val rows = Seq(
        Ciudad("Madrid", 3223334L, "Madrid", "Comunidad de Madrid", 604.3),
        Ciudad(null, 1L, "X", "Y", 1.0)
      ).toDS()

      val refused = intercept[QualityError](Ciudades.validar(rows).count())
      refused.getMessage should include("has 1 null values")
    }

    "keeps the first row per key when deduplicating" in {
      val session = spark
      import session.implicits._
      // The source file has no duplicates, so this needs its own fixture — a
      // dedup step that is never exercised is a dedup step nobody can trust.
      val rows = Seq(
        Ciudad("Madrid", 3223334L, "Madrid", "Comunidad de Madrid", 604.3),
        Ciudad("Madrid", 999L, "Madrid", "Comunidad de Madrid", 604.3),
        Ciudad("Bilbao", 346843L, "Bizkaia", "País Vasco", 41.6)
      ).toDS()

      val out = Ciudades.deduplicar(rows).collect().map(c => c.ciudad -> c.habitantes).toMap
      out should have size 2
      out("Madrid") shouldBe 3223334L
    }
  }

  "end to end" - {

    "reads the file, writes both outputs, and gets the numbers right" in {
      val session = spark
      import session.implicits._

      Ciudades.run(spark, config)
      PorCcaa.run(spark, config)

      val totals = spark.read
        .schema(PorCcaa.produces.schema)
        .option("header", "true")
        .csv(PorCcaa.produces.resolve(config))
        .as[PoblacionCcaa]
        .collect()
        .map(row => row.comunidad_autonoma -> row.habitantes)
        .toMap

      // Computed from the source file independently of Spark.
      totals("Andalucía") shouldBe 4055862L
      totals("Cataluña") shouldBe 3780059L

      // Nothing was lost between the two stages.
      val cities = spark.read
        .schema(Ciudades.produces.schema)
        .option("header", "true")
        .csv(Ciudades.produces.resolve(config))
        .as[Ciudad]
      cities.count() shouldBe 100L
      totals.values.sum shouldBe cities.collect().map(_.habitantes).sum
    }
  }

  "the two checks a type cannot make" - {

    "a path that is not there, named as a path" in {
      // Without this the read returns an empty frame and the failure surfaces as
      // a missing column, which sends you to debug the case class.
      val missing = DataRef[Ciudad]("ausente", "resources/no_existe.csv")
      val refused = intercept[QualityError](Io.read(spark, missing, config))
      refused.getMessage should include("input path does not exist")
    }

    "a header whose columns are in the wrong order" in {
      // The trap worth having a test for: an explicit schema is applied by
      // position, so a file with the right names in the wrong order is read
      // without complaint and every value lands under the wrong heading.
      val swapped = root.resolve("resources/swapped.csv")
      Files.write(
        swapped,
        java.util.Arrays.asList(
          "habitantes,ciudad,provincia,comunidad_autonoma,superficie_km2",
          "3223334,Madrid,Madrid,Comunidad de Madrid,604.3"
        )
      )

      val ref = DataRef[Ciudad]("torcido", "resources/swapped.csv")
      val refused = intercept[QualityError](Io.read(spark, ref, config))
      refused.getMessage should include("in a different order")
      refused.getMessage should include("applied by position")
    }
  }
}
