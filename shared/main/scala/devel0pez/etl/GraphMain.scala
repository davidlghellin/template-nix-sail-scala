package devel0pez.etl

import devel0pez.etl.jobs.Jobs

/** Prints the chain, the way `python -m etl_kedro.graph` does in the Python template.
  *
  * It starts no session and reads no data — the graph is derived from what the jobs declare, so
  * this costs a JVM start and nothing else. Run it before launching to see the order and the paths
  * the current environment resolves to:
  *
  * {{{
  * sbt "classic/runMain devel0pez.etl.GraphMain"
  * sbt "classic/runMain devel0pez.etl.GraphMain --mermaid"
  * ETL_ENV=pro ETL_DATA_ROOT=s3://bucket/gold sbt "classic/runMain devel0pez.etl.GraphMain"
  * }}}
  */
object GraphMain {

  def main(args: Array[String]): Unit = {
    val config = EtlConfig.fromEnv()
    if (args.contains("--mermaid")) println(Jobs.graph.renderMermaid)
    else println(Jobs.graph.render(config))
  }
}
