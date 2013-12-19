package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.DatabaseIndexing
import scala.collection.{mutable, BitSet}

/**
 * A set of vertices. Ligra-style computation.
 * @author Aapo Kyrola
 */
trait VertexFrontier {

   def insert(vertexId: Long) : Unit
   def hasVertex(vertexId: Long): Boolean
   def isEmpty : Boolean
   def size: Int

}



class DenseVertexFrontier(indexing: DatabaseIndexing) extends  VertexFrontier {

  var empty = true
  val shardBitSets = (0 until indexing.nShards).map(i => new scala.collection.mutable.BitSet(indexing.shardSize(i).toInt)).toIndexedSeq

  def insert(vertexId: Long) : Unit = {
    if (empty) empty = false
    shardBitSets(indexing.shardForIndex(vertexId)).+=(indexing.globalToLocal(vertexId).toInt)
  }
  def hasVertex(vertexId: Long) = shardBitSets(indexing.shardForIndex(vertexId))(indexing.globalToLocal(vertexId).toInt)
  def isEmpty = empty
  def size: Int = shardBitSets.map(_.count(i => true)).sum
}


class SparseVertexFrontier(indexing: DatabaseIndexing) extends VertexFrontier {

  val backingSet = new mutable.HashSet[Long] with mutable.SynchronizedSet[Long]

  def insert(vertexId: Long) = backingSet.add(vertexId)

  def hasVertex(vertexId: Long) = backingSet.contains(vertexId)
  def isEmpty = backingSet.isEmpty
  def size = backingSet.size

}