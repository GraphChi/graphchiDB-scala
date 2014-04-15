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

package edu.cmu.graphchidb.compute

import edu.cmu.graphchidb.DatabaseIndexing

/**
 * @author Aapo Kyrola
 */
trait Scheduler {
  /**
   * Asks vertex to be updated on next iteration
   * @param alreadyThisIteration update already on this iteration, if possible (default false)
   */
  def addTask(vertexId: Long, alreadyThisIteration : Boolean = false) = {}
  def addTaskToAll() : Unit = {}

  def isScheduled(vertexId: Long) = true

  // TODO: pass only vertex ids to avoid object creation
  def addTasks[T](edges: Iterator[Long], alreadyThisIteration : Boolean = false) =
    edges.foreach(vid => addTask(vid, alreadyThisIteration))

  def swap : Scheduler = this

  def hasNewTasks : Boolean = true
}



class BitSetScheduler(indexing: DatabaseIndexing) extends  Scheduler {

  assert(indexing.name == "vertex")

  val currentSchedule = (0 until indexing.nShards).map(i => new scala.collection.mutable.BitSet(indexing.shardSize(i).toInt)).toIndexedSeq
  val nextSchedule = (0 until indexing.nShards).map(i => new scala.collection.mutable.BitSet(indexing.shardSize(i).toInt)).toIndexedSeq

  var newTasks: Boolean = false

  override def addTask(vertexId: Long, alreadyThisIteration : Boolean = false) =  {
    val localIdx = indexing.globalToLocal(vertexId).toInt
    nextSchedule(indexing.shardForIndex(vertexId)).+=(localIdx)
    if (alreadyThisIteration) {
      currentSchedule(indexing.shardForIndex(vertexId)).+=(localIdx)
    }
    newTasks = true
  }

  override def hasNewTasks = newTasks

  override def addTaskToAll() = {
    (0 until indexing.nShards).foreach(i => currentSchedule(i).++=(0 until indexing.shardSize(i).toInt))
  }

  override def isScheduled(vertexId: Long) = currentSchedule(indexing.shardForIndex(vertexId))(indexing.globalToLocal(vertexId).toInt)

  override def swap  = {
    val newSchedule = new BitSetScheduler(indexing)
    newSchedule.currentSchedule.zip(this.nextSchedule).foreach(tp => tp._1.++=(tp._2))
    newSchedule
  }

}