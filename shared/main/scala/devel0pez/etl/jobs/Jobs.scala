package devel0pez.etl.jobs

import devel0pez.etl.{Graph, Job}

/** Every job in the chain, in one place.
  *
  * The Python version derives this by walking its `jobs` package. Doing the same here would mean
  * classpath reflection, and it would be a worse trade: a registry the compiler checks cannot name
  * a job that does not exist, and adding a line is cheaper than debugging a scan. It is also the
  * only file that has to change when a job is added, which is the property that mattered.
  */
object Jobs {

  val all: Seq[Job[_, _]] = Seq(Ciudades, PorCcaa)

  def graph: Graph = Graph(all)
}
