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

import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.Util.timed
import edu.cmu.graphchidb.queries.internal.{QueryResultContainer, ResultEdges}


case class VertexId(originalId: Long, internalId: Long)

case class ResultEdge(src: Long, dst: Long, dataPtr: Long)

/**
 * TODO: this needs refactoring
 * @author Aapo Kyrola
 */
class QueryResult(indexing: DatabaseIndexing, result: QueryResultContainer,  database: GraphChiDatabase) {

  lazy val rows = result.combinedResults()

  // TODO: multijoin
  def join[T](column: Column[T]) = {
     if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
    column.indexing match {
      case database.edgeIndexing => {
        val joins1 = database.edgeColumnValues(column, rows.pointers.toSet)
        joins1.keySet map {row => {
          val internalId = rows.idForPointer(row)
          (VertexId(database.internalToOriginalId(internalId), internalId), joins1(row))
        }}
      }
      case database.vertexIndexing => {
        val joins1 = column.getMany(rows.ids.toSet)
        joins1.keySet map {row => (VertexId(database.internalToOriginalId(row), row), joins1(row))}
      }
    }
  }

  def join[T, V](column: Column[T], column2: Column[V]) = {
    if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
    if (column2.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
     val idSet = rows.pointers.toSet
    val joins1 = timed("join1",  column.getMany(idSet))
    val rows2 = idSet.intersect(joins1.keySet)
    val joins2 = timed ("join2", column2.getMany(rows2) )
    joins2.keySet map {row => (rows.idForPointer(row), joins1(row), joins2(row))}
  }


  /* Combined results */
  def getVertices = rows.ids.map(vid => VertexId(database.internalToOriginalId(vid), vid))

  def getInternalIds : Seq[Long] = rows.ids

  def getPointers = rows.pointers

  /* Multiresults */
  def resultsForVertex(vertexId: Long) = result.resultsFor(vertexId)

  def size = result.size

  override def toString() = "Query result: %d rows".format(rows.ids.size)

  def withIndexing(desiredIndexing: DatabaseIndexing) = {
    new QueryResult(desiredIndexing, result, database)
  }
}

                 // Returns a set of ids
