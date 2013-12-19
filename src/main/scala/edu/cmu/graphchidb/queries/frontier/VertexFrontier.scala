package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
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

  def toSparse = {
    val sparseFrontier = new SparseVertexFrontier(indexing)
    (0 until indexing.nShards).map(i => {
      val bits = shardBitSets(i)
      bits.iterator.foreach(v => {
        sparseFrontier.insert(indexing.localToGlobal(i, v))
      } )
    } )
    sparseFrontier
  }
}


class SparseVertexFrontier(indexing: DatabaseIndexing) extends VertexFrontier {

  val backingSet = new mutable.HashSet[Long] with mutable.SynchronizedSet[Long]

  def this(indexing: DatabaseIndexing, set: Set[Long]) {
      this(indexing)
      backingSet ++= set
  }


  def insert(vertexId: Long) = backingSet.add(vertexId)

  def hasVertex(vertexId: Long) = backingSet.contains(vertexId)
  def isEmpty = backingSet.isEmpty
  def size = backingSet.size
  def toSeq : Seq[Long] = backingSet.toSeq

  def toDense = {
    val denseFrontier = new DenseVertexFrontier(indexing)
    backingSet.foreach(id => denseFrontier.insert(id))
    denseFrontier
  }
}

object VertexFrontier {

  val sparseLimit = 10000

  def createFrontier(internalIds: Seq[java.lang.Long], db: GraphChiDatabase) = {
      if (internalIds.size > sparseLimit) {
         val dense = new DenseVertexFrontier(db.vertexIndexing)
         internalIds.foreach(id => dense.insert(id))
         dense
      } else {
         new SparseVertexFrontier(db.vertexIndexing, internalIds.toSet[Long])
      }
  }

}