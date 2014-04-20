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

package edu.cmu.graphchidb.examples.advanced

import edu.cmu.graphchidb.{GraphChiDatabase, GraphChiDatabaseAdmin}
import edu.cmu.graphchidb.compute.{GraphChiContext, GraphChiVertex, VertexCentricComputation}
import edu.cmu.graphchi.bits.CompactBoundedCounterVector
import edu.cmu.graphchidb.Util._
import java.io.FileWriter

/**
 * Computes multiple BFS'es in parallel.
 * @author Aapo Kyrola
 */
object MultiBFS {

  /**
  import edu.cmu.graphchidb.examples.advanced.MultiBFS._
   runForRandomSeeds


    */

  val baseFilename = System.getProperty("user.home")  + "/graphs/DB/livejournal/lj"

  implicit val DB = new GraphChiDatabase(baseFilename,  numShards = 16)
  DB.initialize()

  val numBFSinParallel = 128  /// Note: for good performance, you should have enough memory to hold numVertices * 3 * numBFSinParallel BITS
  val multiBFSComp = new MultiBFSComputation(DB)

  def runForRandomSeeds() = {
    async {
      // Seeds
      val seeds = (0 until numBFSinParallel).map(i => DB.randomVertex())
      println("Seeds: " + seeds)
      multiBFSComp.setSeeds(seeds)

      DB.runGraphChiComputation(multiBFSComp, numIterations = 6, enableScheduler = true)
      println("Computation ready. Computing result....")


      val bfsCounters = new Array[Array[Int]](numBFSinParallel)
      for(i <- 0 until numBFSinParallel) {  bfsCounters(i) = new Array[Int](6) }

      multiBFSComp.bfsLevelColumn.foreach((vertexId, counters) => {
        for(i <- 0 until numBFSinParallel) {
          val n = counters.get(i)
          if (n > 1) {
            bfsCounters(i)(n - 2) += 1
          }
        }
      })


      // Compute the bfs level distributions
      val filename = "bfslevels_%d.txt".format(System.currentTimeMillis())
      val fw = new FileWriter(filename)
      fw.write("vertex,")
      for(i <- 0 until bfsCounters(0).length) {
        fw.write("level%d,".format(i))
      }
      fw.write("\n")
        for(i <- 0 until numBFSinParallel) {
         fw.write("%d,".format(DB.internalToOriginalId(multiBFSComp.seeds(i))))
         bfsCounters(i).foreach(j => fw.write("%d,".format(j)))
         fw.write("\n")
      }
      fw.close()

      println("Results in file " + filename)
    }
  }

}


class MultiBFSComputation(DB: GraphChiDatabase) extends VertexCentricComputation[CompactBoundedCounterVector, AnyRef] {
  val numBits = 3
  val bfsLevelColumn = DB.createCustomTypeColumn[CompactBoundedCounterVector]("multibfs", DB.vertexIndexing,
    new CompactBoundedCounterVector(MultiBFS.numBFSinParallel, numBits).getByteConverter, temporary = false)

  var seeds: Array[Long] = Array[Long]()

  /**
   * Update function to be implemented by an algorithm
   * @param vertex
   * @param context
   */
  def update(vertex: GraphChiVertex[CompactBoundedCounterVector, AnyRef], context: GraphChiContext) = {

    // Logic: take the minimum non-zero counter from neighbors (incremented by one), and my own counters
    var before = bfsLevelColumn.get(vertex.id).get
    val curMin = vertex.edgeVertexIds.foldLeft(before)((cur, nbid) => {
      val nbrCounters = bfsLevelColumn.get(nbid).get
      CompactBoundedCounterVector.pointwiseMinOfNonzeroesIncrementByOne(cur, nbrCounters, true)
    })

    if (context.iteration == 0 || !equals(curMin, before) ) {
      bfsLevelColumn.set(vertex.id, curMin)
      // Schedule neighbors
      context.scheduler.addTasks(vertex.edgeVertexIds)
    }
  }

  override def beforeIteration(context: GraphChiContext) = {

  }

  def setSeeds(seedVerticesInternalIds: Seq[Long]) = {
    if (seedVerticesInternalIds.length != MultiBFS.numBFSinParallel) throw new IllegalArgumentException("The number of seeds must match the number of parallel BFS")
    this.seeds = seedVerticesInternalIds.toArray

    // Reset values
    bfsLevelColumn.fillWithZeroes

    // Set seeds counters
    seeds.zipWithIndex.foreach { case (seedId: Long, idx: Int) => {
      val counter = new CompactBoundedCounterVector(MultiBFS.numBFSinParallel, numBits)
      counter.increment(idx)
      bfsLevelColumn.set(seedId, counter)
    }}
  }


  override def  isParallel = true


  def edgeDataColumn = None

  def vertexDataColumn = Some(bfsLevelColumn)
}
