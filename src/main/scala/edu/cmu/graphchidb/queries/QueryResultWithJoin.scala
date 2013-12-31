package edu.cmu.graphchidb.queries

import edu.cmu.graphchi.queries.QueryCallback
import edu.cmu.graphchidb.storage.Column
import java.{lang, util}
import edu.cmu.graphchidb.GraphChiDatabase
import scala.collection.mutable.ArrayBuffer

/**
 * @author Aapo Kyrola
 */
class QueryResultWithJoin[T](database: GraphChiDatabase, joinFunction: (Long, Long, Byte, Long) => T) extends QueryCallback {

  var results = ArrayBuffer[T]()

  def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
       val joinedValue = joinFunction(src, dst, edgeType, dataPtr)
      this.synchronized {
        results.append(joinedValue)
      }

  }


  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long],
                         edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])
     = throw new IllegalStateException("Immediate result expected!")

  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])
     = throw new IllegalStateException("Immediate result expected!")

  def immediateReceive() = true

  def get = results
}
