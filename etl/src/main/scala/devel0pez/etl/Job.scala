package devel0pez.etl

import java.nio.file.{Files, Paths}

import org.apache.spark.sql.{Dataset, SparkSession}

import devel0pez.Conform._

/** Raised when data does not meet the contract — as opposed to when the code is wrong. */
final class QualityError(message: String) extends RuntimeException(message)

/** One step of the chain: what it consumes, what it produces, and the transformation between.
  *
  * `CONSUME` and `PRODUCE` in the Python version are tuples of catalogue entries, checked by a
  * dry-run that walks the modules and compares `StructType`s. Here they are `DataRef[In]` and
  * `DataRef[Out]`, and the check is the type parameter: a job consuming `Ciudad` can only be fed
  * something that produces `Ciudad`. Chaining two jobs whose shapes disagree is a compile error
  * with a line number, not a dry-run finding.
  *
  * `transform` is the domain, and it is deliberately the only thing a job author writes by hand:
  * `Dataset[In] => Dataset[Out]`, no session, no paths, no I/O. That is what makes it testable
  * against either engine without a fixture on disk.
  */
trait Job[In, Out] {

  /** The name that appears in the graph and on the command line. */
  def name: String

  def consumes: DataRef[In]

  def produces: DataRef[Out]

  /** The domain. Pure: in a `Dataset`, out a `Dataset`, nothing else. */
  def transform(input: Dataset[In]): Dataset[Out]

  /** Read, transform, write. Rarely overridden — the point is that it is the same every time. */
  def run(spark: SparkSession, config: EtlConfig): Dataset[Out] = {
    val input = Io.read(spark, consumes, config)
    val output = transform(input)
    Io.write(output, produces, config)
    output
  }
}

/** Reading and writing a `DataRef`, with the checks that a type cannot make.
  *
  * Everything here is about the gap between a case class and a file. `Conform[T]` guarantees the
  * frame becomes a `Dataset[T]` or fails saying why; what it cannot guarantee is that the file
  * behind the path exists, or that its header is in the order an explicit schema will assume.
  */
object Io {

  def read[T](spark: SparkSession, ref: DataRef[T], config: EtlConfig): Dataset[T] = {
    implicit val shape: devel0pez.Conform[T] = ref.shape
    val path = ref.resolve(config)
    checkExists(ref.name, path)

    ref.format match {
      // Parquet carries its own schema, so there is no header to contrast and
      // nothing to infer.
      case Format.Parquet => spark.read.parquet(path).conformTo[T]
      case Format.Csv     =>
        // Checked here rather than left to the reader, because the reader's
        // complaint is a parse error that reads like a bug in the code. This
        // one names the column and the file.
        EtlConfig.csvHeader(path).flatMap(EtlConfig.headerProblem(ref.schema, _)).foreach {
          problem =>
            throw new QualityError(s"[${ref.name}] $problem")
        }
        spark.read
          .schema(ref.schema)
          .option("header", "true")
          .csv(path)
          .conformTo[T]
    }
  }

  def write[T](ds: Dataset[T], ref: DataRef[T], config: EtlConfig): Unit = {
    implicit val shape: devel0pez.Conform[T] = ref.shape
    val path = ref.resolve(config)
    val conformed = ds.conformTo[T]
    ref.format match {
      case Format.Parquet => conformed.write.mode("overwrite").parquet(path)
      case Format.Csv     => conformed.write.mode("overwrite").option("header", "true").csv(path)
    }
  }

  /** A missing input, caught before Spark starts.
    *
    * Without this a mistyped path reads as an empty frame with no columns, and the error that
    * surfaces is "missing columns" — which sends the reader to debug the schema when the problem is
    * the path.
    */
  def checkExists(name: String, path: String): Unit =
    if (EtlConfig.isLocal(path) && !Files.exists(Paths.get(path))) {
      throw new QualityError(s"[$name] input path does not exist: $path")
    }
}

/** The chain, derived from what the jobs declare rather than maintained by hand.
  *
  * The Python version discovers jobs by walking the package with `pkgutil`. This one is handed the
  * list, and that is the honest difference: Scala could reflect over the classpath, but a registry
  * that the compiler checks beats a scan that fails at run time when a name is wrong. The cost is
  * one line per job in `Jobs.all`; the benefit is that a job that does not compile cannot be in it.
  *
  * There is deliberately no dry-run here, and its absence is the clearest measure of what the types
  * bought. The Python version needs one because its central question — does the schema this job
  * produces match the one the next job consumes — can only be answered by loading both modules and
  * comparing two `StructType`s. Here that question is answered by `scalac`: `Job[Ciudad, ?]` feeds
  * `Job[Ciudad, ?]` or the build fails. What was left over — a missing file, a header in the wrong
  * order — belongs at the point of use, and lives in `Io.read`. What remains after that is a
  * property of this graph, and `EtlChainSpec` asserts it: the run of the chain **is** the dry-run.
  */
final case class Graph(jobs: Seq[Job[_, _]]) {

  /** Datasets more than one job claims to write.
    *
    * A graph with one of these is not a graph with a warning in it, it is a wrong graph: the
    * mapping below can only hold one producer per name, so the loser vanishes and the edges,
    * external inputs and both renderings come out describing a chain nobody wrote. It used to do
    * exactly that in silence — a second job pointed at `ciudades_dedup` dropped `ciudades` from the
    * picture and left `edges` empty.
    */
  def conflicts: Seq[(String, Seq[String])] =
    jobs
      .groupBy(_.produces.name)
      .toSeq
      .collect { case (name, owners) if owners.size > 1 => name -> owners.map(_.name).sorted }
      .sortBy(_._1)

  /** This graph, or a failure naming the datasets with more than one producer.
    *
    * `Jobs.graph` goes through here, so the ordinary way of getting a graph cannot yield a silently
    * wrong one. The plain constructor stays unchecked on purpose: a test has to be able to build a
    * conflicting graph in order to assert what happens to it.
    */
  def validated: Graph = {
    if (conflicts.nonEmpty) {
      val detail = conflicts
        .map { case (dataset, owners) => s"$dataset is produced by ${owners.mkString(" and ")}" }
        .mkString("; ")
      throw new IllegalArgumentException(s"a dataset can only have one producer: $detail")
    }
    this
  }

  /** dataset name -> the job that writes it.
    *
    * Assumes `conflicts` is empty; see `validated` for why that is not left to chance.
    */
  def producerOf: Map[String, String] =
    jobs.map(job => job.produces.name -> job.name).toMap

  /** (upstream job, downstream job, the dataset that links them). */
  def edges: Seq[(String, String, String)] =
    for {
      job <- jobs
      producer <- producerOf.get(job.consumes.name).toSeq
    } yield (producer, job.name, job.consumes.name)

  /** Inputs nobody in this chain produces: where the data comes from. */
  def externalInputs: Seq[String] =
    jobs.map(_.consumes.name).filterNot(producerOf.contains).distinct

  /** Outputs nobody in this chain consumes: what the chain is for. */
  def finalOutputs: Seq[String] = {
    val consumed = jobs.map(_.consumes.name).toSet
    jobs.map(_.produces.name).filterNot(consumed.contains).distinct
  }

  /** Dependency order, or `None` when the declarations describe a cycle. */
  def order: Option[Seq[String]] = {
    val incoming = edges.groupBy(_._2).view.mapValues(_.map(_._1).toSet).toMap
    def loop(done: Seq[String], left: Seq[Job[_, _]]): Option[Seq[String]] =
      if (left.isEmpty) Some(done)
      else {
        val (ready, blocked) =
          left.partition(job => incoming.getOrElse(job.name, Set.empty).subsetOf(done.toSet))
        // Nothing became ready and something is left: every remaining job is
        // waiting on another one, which is what a cycle looks like from here.
        if (ready.isEmpty) None
        else loop(done ++ ready.map(_.name), blocked)
      }
    loop(Seq.empty, jobs)
  }

  def render(config: EtlConfig): String = {
    val lines = jobs.map { job =>
      val from = producerOf
        .get(job.consumes.name)
        .fold("(external)")(producer => s"(from $producer)")
      s"""${job.name}
         |  <- ${job.consumes.name.padTo(24, ' ')}${job.consumes.resolve(config)}  $from
         |  -> ${job.produces.name.padTo(24, ' ')}${job.produces.resolve(config)}""".stripMargin
    }
    val chain = edges.map { case (from, to, via) => s"  $from -> $to   ($via)" }
    (lines ++
      Seq("Chain:") ++ (if (chain.isEmpty) Seq("  (single job)") else chain) ++
      Seq(
        s"External inputs: ${externalInputs.mkString(", ")}",
        s"Final outputs:   ${finalOutputs.mkString(", ")}"
      )).mkString("\n")
  }

  /** The same graph as mermaid, which GitHub renders in place.
    *
    * Generated from the same `consumes`/`produces` as the text form, so the two cannot disagree —
    * which is the whole reason neither is drawn by hand.
    */
  def renderMermaid: String = {
    val nodes = jobs.flatMap { job =>
      Seq(s"    ${job.name}([${job.name}])") ++
        Seq(s"    ${job.consumes.name}[(${job.consumes.name})] --> ${job.name}") ++
        Seq(s"    ${job.name} --> ${job.produces.name}[(${job.produces.name})]")
    }
    val classes = Seq(
      "",
      "    classDef job fill:#2d6a9f,stroke:#1b3f5e,color:#fff",
      "    classDef external fill:#7a5c2e,stroke:#4a3619,color:#fff",
      "    classDef terminal fill:#2f6b4f,stroke:#1c4130,color:#fff",
      s"    class ${jobs.map(_.name).mkString(",")} job"
    ) ++
      (if (externalInputs.nonEmpty) Seq(s"    class ${externalInputs.mkString(",")} external")
       else Nil) ++
      (if (finalOutputs.nonEmpty) Seq(s"    class ${finalOutputs.mkString(",")} terminal") else Nil)
    (Seq("flowchart LR") ++ nodes ++ classes).mkString("\n")
  }
}
