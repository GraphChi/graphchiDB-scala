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

  private val results = mutable.Map[Long, ResultEdges]() ++ queryIds.map(i => i ->
    ResultEdges(IndexedSeq[java.lang.Long](), IndexedSeq[java.lang.Long]())).toMap

  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], dataPointers: util.ArrayList[lang.Long]) = {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        results(vertexId) += ResultEdges(neighborIds.toIndexedSeq[java.lang.Long], dataPointers.toIndexedSeq[java.lang.Long])
      }
    }
  }

  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], dataPointers: util.ArrayList[lang.Long])= {
    if (!neighborIds.isEmpty) {
      this.synchronized {
        results(vertexId) += ResultEdges(neighborIds.toIndexedSeq[java.lang.Long], dataPointers.toIndexedSeq[java.lang.Long])
      }
    }
  }

  def resultsFor(queryId: Long) = results(queryId)

  def combinedResults() = results.values.reduce(_+_)
}

case class ResultEdges(ids: IndexedSeq[java.lang.Long], pointers: IndexedSeq[java.lang.Long]) {
  def +(that: ResultEdges) = ResultEdges(ids ++ that.ids, pointers ++ that.pointers)
  def size = ids.size
}