/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */


package edu.cmu.graphchidb.examples

import edu.cmu.graphchidb.{Util, GraphChiDatabase, GraphChiDatabaseAdmin}
import edu.cmu.graphchidb.queries.{ResultEdge, Queries}
import scala.util.Random
import java.io.FileWriter

/**
 * Example application for GraphChi-DB that samples vertices from the graph, loads for each vertex
 * the induced neighborhood graph (edges that span between the neighbors of the ego-vertex, excluding the ego),
 * and then samples three-vertex induces subgraphs from the neighborhood graph and counts the subgraph
 * frequencies of the four possible 3-vertex graphs.
 *
 * It should be straightforward to extend this for 4-vertex subgraphs as well.
 *
 * This application is based on the following paper and allows reproducing some of the results:
 *
 * bibtex: @inproceedings{ugander2013subgraph,
  title={Subgraph frequencies: Mapping the empirical and extremal geography of large graph collections},
  author={Ugander, Johan and Backstrom, Lars and Kleinberg, Jon},
  booktitle={Proceedings of the 22nd international conference on World Wide Web},
  pages={1307--1318},
  year={2013},
  organization={International World Wide Web Conferences Steering Committee}

 */

object SubgraphFrequencies {

  /**
  // Usage:
    import edu.cmu.graphchidb.examples.SubgraphFrequencies._

    // To compute subgraph freqs of a given vertex neighborhood
    computeThreeVertexSubgraphFrequencies(inducedNeighborhoodGraph(2419))

   // To produce data similar to used in Figure 1 of Ugander et. al.:
    computeDistribution(500)

    // R-script to plot the results
    library("scatterplot3d")
    df = read.table("subgraphs.txt", sep=",", header=FALSE)

    triangles <- df[, 4]
    oneedges <- df[, 2]
    empty <- df[, 1]

    scatterplot3d(triangles, empty, oneedges,  highlight.3d = TRUE,  angle = 30,
            col.axis = "blue", col.grid = "lightblue", cex.axis = 1.3,
            cex.lab = 1.1, main = "Subgraph Frequencies", pch = 20
)


  */



  /* Input graph: change to match your graph */
  val sourceFile =  System.getProperty("user.home")  + "/graphs/soc-LiveJournal1.txt"
  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/livejournal/lj"
  val numShards = 16

  /* NOTE: to create the database, see SocialNetworkExample.scala */

  implicit val DB = new GraphChiDatabase(baseFilename,  numShards = numShards)

  DB.initialize()

  def inducedNeighborhoodGraph(origVertexId: Long): Set[ResultEdge] = {
    val internalId = DB.originalToInternalId(origVertexId)
    val friends = DB.queryOut(internalId, 0)

    val directedgraph = Queries.inducedSubgraph(friends.getInternalIds.toSet - internalId, 0)  // One might be friend of himself, to remove it
    // map so that src < dst and remove pointer, then transform to a set to remove duplicates
    // also remove self-edges
    directedgraph.map(e => ResultEdge(math.min(e.src, e.dst), math.max(e.src, e.dst), 0) ).filterNot(e => e.src == e.dst).toSet
  }



  /**
   * Returns a four-element vector x, so that x[i] has the fraction of 3-vertex subgraphs with i edges
   * @param subgraph
   */
  def computeThreeVertexSubgraphFrequencies(subgraph: Set[ResultEdge], numSamples: Int = 11000) : Array[Double] = {
    val freqs = new Array[Int](4)
    /* Find the vertex IDs in the subgraph */
    val vertices = (subgraph.map {_.src} ++ subgraph.map {_.dst}).toSet.toIndexedSeq

    if (vertices.size < 4) {  // Do not do trivial
      return new Array[Double](4)
    }

    for(i <- 0 until numSamples) {
       val sampledVertices = Util.randomIndices(vertices.size, 3) map { j => vertices(j) }
       val sampledEdges = subgraph.filter { e => sampledVertices.contains(e.src) && sampledVertices.contains(e.dst)}
       if (sampledEdges.size == 4) println(sampledEdges)
       freqs(sampledEdges.size) += 1
    }
    freqs.map { f => f.toDouble / freqs.sum }
  }


    /**
      *  Samples N vertices from the graph and computes their subgraph freq vector. Outputs to file "subgraphs.txt".
      * Skips trivial vertices (too small neighborhoods).
      */
   def computeDistribution(N: Int = 500) = {
     val numVertices = DB.numVertices
     var n = 0
     val fw = new FileWriter("subgraphs.txt")

     while(n < N) {
       val randomVertexOrigId = Random.nextInt(numVertices.toInt)
       val freqs = computeThreeVertexSubgraphFrequencies(inducedNeighborhoodGraph(randomVertexOrigId))

       if (freqs.sum > 0) {
         fw.write("%f,%f,%f,%f\n".format(freqs(0), freqs(1), freqs(2), freqs(3)))
         n += 1
       }
       println("%d/%d".format(n, N))
     }
      fw.close()
      println("Results in subgraphs.txt")
  }

}