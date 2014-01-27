package edu.cmu.graphchidb.queries.internal
import scala.collection.JavaConversions._

import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import edu.cmu.graphchidb.GraphChiDatabase

/**
 * A callback object for query shards, encapsulates the results.
 * TODO: think if better to use immutable map with var or mutable map
 * @author Aapo Kyrola
 */
class QueryResultContainer(queryIds: Set[Long]) extends QueryCallback {


  //private val results = mutable.Map[Long, ResultEdges]() ++ queryIds.map(i => i ->
  //ResultEdges(IndexedSeq[java.lang.Long](), IndexedSeq[java.lang.Long]())).toMap

  private var resultList = List[Tuple2[Long, ResultEdges]]()


  def immediateReceive = false


  def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = throw new IllegalStateException()

  def receiveOutNeighbors(vertexId: Long, neighborIdsJava: util.ArrayList[lang.Long],
                          edgeTypesJava: util.ArrayList[lang.Byte],
                          dataPointersJava: util.ArrayList[lang.Long]) : Unit = {
       val neighborIds = new Array[Long](neighborIdsJava.size())
       val edgeTypes = new Array[Byte](edgeTypesJava.size())
       val dataPointers = new Array[Long](dataPointersJava.size())
       val n = neighborIds.size
       var i = 0
       while(i < n) {
          neighborIds(i) = neighborIdsJava(i)
          edgeTypes(i) = edgeTypesJava(i)
          dataPointers(i) = dataPointersJava(i)
          i += 1
       }
       receiveOutNeighbors(vertexId, neighborIds, edgeTypes, dataPointers)
  }


  def receiveInNeighbors(vertexId: Long, neighborIdsJava: util.ArrayList[lang.Long],
                         edgeTypesJava: util.ArrayList[lang.Byte],
                         dataPointersJava: util.ArrayList[lang.Long]) : Unit = {
    val neighborIds = new Array[Long](neighborIdsJava.size())
    val edgeTypes = new Array[Byte](edgeTypesJava.size())
    val dataPointers = new Array[Long](dataPointersJava.size())
    val n = neighborIds.size
    var i = 0
    while(i < n) {
      neighborIds(i) = neighborIdsJava(i)
      edgeTypes(i) = edgeTypesJava(i)
      dataPointers(i) = dataPointersJava(i)
      i += 1
    }
    receiveInNeighbors(vertexId, neighborIds, edgeTypes, dataPointers)
  }

  def receiveInNeighbors(vertexId: Long, neighborIds: Array[Long], types: Array[Byte], dataPointers: Array[Long]) = {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        resultList = resultList :+ (vertexId, ResultEdges(neighborIds, types, dataPointers))
      }
    }
  }

  // TODO: use flat arrays?
  def receiveOutNeighbors(vertexId: Long, neighborIds: Array[Long], types: Array[Byte], dataPointers: Array[Long])= {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        resultList = resultList :+ (vertexId, ResultEdges(neighborIds, types, dataPointers))
      }
    }
  }

  // Unscalaish --- but the basic merge is too slow for very large results
  private def fastMerge(results: Seq[ResultEdges]) : ResultEdges = {
    val totalLen = results.map(_.size).sum
    val ids = new Array[Long](totalLen)
    val types = new Array[Byte](totalLen)
    val pointers = new Array[Long](totalLen)
    var i = 0
    results.foreach( r => {
      r.ids.copyToArray(ids, i)
      r.types.copyToArray(types, i)
      r.pointers.copyToArray(pointers, i)
      i += r.size
    })
    assert(i == totalLen)
    ResultEdges(ids, types, pointers)
  }


  def results = {
    val grouped = resultList.groupBy(_._1).map { case (queryId, idTuple) => (queryId, fastMerge(idTuple.map(_._2))) }
    grouped.toMap
  }

  def resultsFor(queryId: Long) = results.getOrElse(queryId, ResultEdges(Seq[Long](), Seq[Byte](), Seq[Long]()))

  def combinedResults() =  results.values.foldLeft(ResultEdges(Seq[Long](), Seq[Byte](), Seq[Long]()))(_+_)

  def size = results.values.foldLeft(0)(_ + _.size)
}

case class ResultEdges(ids: Seq[Long], types: Seq[Byte], pointers: Seq[Long]) {
  def +(that: ResultEdges) = ResultEdges(ids ++ that.ids, types ++ that.types, pointers ++ that.pointers)
  def size = ids.size
  def idForPointer(pointer: java.lang.Long) = ids(pointers.indexOf(pointer)) // Note, optimize for large result sets!
}

class SimpleSetReceiver(outEdges: Boolean) extends QueryCallback {
  val set = new collection.mutable.HashSet[Long] with mutable.SynchronizedSet[Long]
  def immediateReceive() = true
  def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
    if (outEdges) set += dst
    else set += src
  }

  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()

}



class SimpleArrayReceiver(outEdges: Boolean) extends QueryCallback {
  val arr = new ArrayBuffer[Long] with mutable.SynchronizedBuffer[Long]
  def immediateReceive() = true
  def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
    if (outEdges) arr += dst
    else arr += src
  }

  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()
  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= throw new IllegalStateException()

}