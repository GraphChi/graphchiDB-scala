package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.{GraphChiDatabase, DatabaseIndexing}
import edu.cmu.graphchidb.Util.timed
import edu.cmu.graphchidb.queries.internal.ResultEdges


case class VertexId(originalId: Long, internalId: Long)

/**
 * @author Aapo Kyrola
 */
class QueryResult(indexing: DatabaseIndexing, rows: ResultEdges, database: GraphChiDatabase) {

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


  def getVertices = rows.ids.map(vid => VertexId(database.originalToInternalId(vid), vid))

  def getInternalIds = rows.ids

  override def toString() = "Query result: %d rows".format(rows.ids.size)

  def withIndexing(desiredIndexing: DatabaseIndexing) = {
    new QueryResult(desiredIndexing, rows, database)
  }
}


