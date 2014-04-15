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
