package edu.cmu.graphchidb.queries.frontier

import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import scala.collection.{mutable, BitSet}

/**
 * A set of vertices. Ligra-style computation.
 * @author Aapo Kyrola
 */
trait VertexFrontier {

  def db : GraphChiDatabase

  def insert(vertexId: Long) : Unit
  def hasVertex(vertexId: Long): Boolean
  def hasAnyVertex(other: VertexFrontier) : Option[Long]
  def isEmpty : Boolean
  def size: Int


  def toSet: Set[Long]

  def apply[T](f : VertexFrontier => T) = f(this)

  def ->[T](f : VertexFrontier => T) = f(this)

}



class DenseVertexFrontier(indexing: DatabaseIndexing, db_ : GraphChiDatabase) extends  VertexFrontier {

  def db = db_

  var counter = 0
  var empty = true
  var shardBitSets = (0 until indexing.nShards).map(i => new scala.collection.mutable.BitSet(indexing.shardSize(i).toInt)).toIndexedSeq

  def union(frontier: VertexFrontier) : Unit = {
    frontier match {
      case dense: DenseVertexFrontier => {
        shardBitSets = (0 until indexing.nShards).map(i => this.shardBitSets(i) | dense.shardBitSets(i))
      }
      case sparse: SparseVertexFrontier => {
         sparse.toSet.foreach(x => insert(x))
      }
    }
  }


  def hasAnyVertex(other: VertexFrontier) : Option[Long] = {
    other match {
      case dense: DenseVertexFrontier => {
        (0 until indexing.nShards).foreach(i => {
           val intersection = this.shardBitSets(i) & dense.shardBitSets(i)
           val iterator = intersection.iterator
           if (iterator.hasNext) {
               return Some(indexing.localToGlobal(i, iterator.next()))
           }
        })
        None
      }
      case sparse: SparseVertexFrontier => {
        sparse.toSet.foreach(x => {
            if (hasVertex(x)) return Some(x)
        })
        None
      }
    }
  }

  def insert(vertexId: Long) : Unit = {
     val bitset = shardBitSets(indexing.shardForIndex(vertexId))
     val localIdx = indexing.globalToLocal(vertexId).toInt
     if (!bitset(localIdx)) {
       bitset.+=(localIdx)
       counter += 1 // not thread-safe!
     }
  }
  def hasVertex(vertexId: Long) = shardBitSets(indexing.shardForIndex(vertexId))(indexing.globalToLocal(vertexId).toInt)
  def isEmpty = counter == 0
  def size: Int = counter  // shardBitSets.map(_.count(i => true)).sum

  def toSparse = {
    val sparseFrontier = new SparseVertexFrontier(indexing, db_)
    (0 until indexing.nShards).map(i => {
      val bits = shardBitSets(i)
      bits.iterator.foreach(v => {
        sparseFrontier.insert(indexing.localToGlobal(i, v))
      } )
    } )
    sparseFrontier
  }

  def toSet = toSparse.toSet
}


class SparseVertexFrontier(indexing: DatabaseIndexing, db_ :GraphChiDatabase) extends VertexFrontier {

  def db = db_

  val backingSet = new mutable.HashSet[Long] with mutable.SynchronizedSet[Long]

  def this(indexing: DatabaseIndexing, set: Set[Long], db_ :GraphChiDatabase) {
      this(indexing, db_)
      backingSet ++= set
  }


  def hasAnyVertex(other: VertexFrontier) : Option[Long] = {
    other match {
      case dense: DenseVertexFrontier => dense.hasAnyVertex(this)
      case sparse: SparseVertexFrontier => {
        sparse.toSet.foreach(x => {
          if (hasVertex(x)) return Some(x)
        })
        None
      }
    }
  }

  def insert(vertexId: Long) = backingSet.add(vertexId)

  def hasVertex(vertexId: Long) = backingSet.contains(vertexId)
  def isEmpty = backingSet.isEmpty
  def size = backingSet.size
  def toSet : Set[Long] = backingSet.toSet

  def toDense = {
    val denseFrontier = new DenseVertexFrontier(indexing, db_)
    backingSet.foreach(id => denseFrontier.insert(id))
    denseFrontier
  }
}

object VertexFrontier {

  val sparseLimit = 100

  def createFrontier(internalIds: Seq[Long], db: GraphChiDatabase) = {
      if (internalIds.size > sparseLimit) {
         val dense = new DenseVertexFrontier(db.vertexIndexing, db)
         internalIds.foreach(id => dense.insert(id))
         dense
      } else {
        // TODO: get rid of the java-long stuff
         new SparseVertexFrontier(db.vertexIndexing, internalIds.toSet, db)
      }
  }

}