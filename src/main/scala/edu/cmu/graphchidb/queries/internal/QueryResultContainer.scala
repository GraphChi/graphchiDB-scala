package edu.cmu.graphchidb.queries.internal
import scala.collection.JavaConversions._

import edu.cmu.graphchi.queries.QueryCallback
import java.{lang, util}
import scala.collection.mutable

/**
 * A callback object for query shards, encapsulates the results.
 * TODO: think if better to use immutable map with var or mutable map
 * @author Aapo Kyrola
 */
class QueryResultContainer(queryIds: Set[java.lang.Long]) extends QueryCallback {

  //private val results = mutable.Map[Long, ResultEdges]() ++ queryIds.map(i => i ->
  //ResultEdges(IndexedSeq[java.lang.Long](), IndexedSeq[java.lang.Long]())).toMap

  private var resultList = List[Tuple2[Long, ResultEdges]]()

  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], types: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long]) = {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        resultList = resultList :+ (vertexId, ResultEdges(neighborIds, types, dataPointers))
      }
    }
  }

  // TODO: use flat arrays?
  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], types: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])= {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        resultList = resultList :+ (vertexId, ResultEdges(neighborIds, types, dataPointers))
      }
    }
  }

  // Unscalaish --- but the basic merge is too slow for very large results
  private def fastMerge(results: Seq[ResultEdges]) : ResultEdges = {
    val totalLen = results.map(_.size).sum
    val ids = new Array[lang.Long](totalLen)
    val types = new Array[lang.Byte](totalLen)
    val pointers = new Array[lang.Long](totalLen)
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


  lazy val results = {
    val grouped = resultList.groupBy(_._1).map { case (queryId, idTuple) => (queryId, fastMerge(idTuple.map(_._2))) }
    grouped.toMap
  }

  def resultsFor(queryId: Long) = results.getOrElse(queryId, ResultEdges(Seq[lang.Long](), Seq[lang.Byte](), Seq[lang.Long]()))

  def combinedResults() =  results.values.foldLeft(ResultEdges(Seq[lang.Long](), Seq[lang.Byte](), Seq[lang.Long]()))(_+_)
}

case class ResultEdges(ids: Seq[java.lang.Long], types: Seq[lang.Byte], pointers: Seq[java.lang.Long]) {
  def +(that: ResultEdges) = ResultEdges(ids ++ that.ids, types ++ that.types, pointers ++ that.pointers)
  def size = ids.size
  def idForPointer(pointer: java.lang.Long) = ids(pointers.indexOf(pointer)) // Note, optimize for large result sets!
}