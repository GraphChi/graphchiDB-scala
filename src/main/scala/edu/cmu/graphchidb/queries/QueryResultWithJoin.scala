package edu.cmu.graphchidb.queries

import edu.cmu.graphchi.queries.QueryCallback
import edu.cmu.graphchidb.storage.Column
import java.{lang, util}
import edu.cmu.graphchidb.GraphChiDatabase
import scala.collection.mutable.ArrayBuffer
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * @author Aapo Kyrola
 */
class QueryResultWithJoin[T](database: GraphChiDatabase, joinFunction: (Long, Long, Byte, Long) => T, initialSize:Int=10) extends QueryCallback {

  val results = new ArrayBuffer[T](initialSize)

  def receiveEdge(src: Long, dst: Long, edgeType: Byte, dataPtr: Long) = {
     throw new NotImplementedException
  }


  def receiveInNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long],
                         edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])
  = throw new NotImplementedException

  def receiveOutNeighbors(vertexId: Long, neighborIds: util.ArrayList[lang.Long], edgeTypes: util.ArrayList[lang.Byte], dataPointers: util.ArrayList[lang.Long])
  = {
    // Unscalaish for performance
     var i = 0
     val n = neighborIds.size()
     val newArr = new ArrayBuffer[T](n)
     while(i < n) {
        newArr += joinFunction(vertexId, neighborIds.get(i), edgeTypes.get(i), dataPointers.get(i))
        i += 1
     }
    this.synchronized {
        results.appendAll(newArr)
    }
  }

  def immediateReceive() = false

  def get = results
}
