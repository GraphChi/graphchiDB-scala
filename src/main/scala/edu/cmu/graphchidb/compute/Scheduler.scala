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