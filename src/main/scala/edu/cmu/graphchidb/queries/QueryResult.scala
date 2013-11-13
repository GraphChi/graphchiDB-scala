package edu.cmu.graphchidb.queries

import edu.cmu.graphchidb.storage.Column
import edu.cmu.graphchidb.DatabaseIndexing
import edu.cmu.graphchidb.Util.timed
import edu.cmu.graphchidb.queries.internal.ResultEdges

/**
 * @author Aapo Kyrola
 */
class QueryResult(indexing: DatabaseIndexing, rows: ResultEdges) {

  // TODO: multijoin
  def join[T](column: Column[T]) = {
    if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
    val joins1 = column.getMany(rows.ids.toSet)

    joins1.keySet map {row => (row, joins1(row))}
  }

  def join[T, V](column: Column[T], column2: Column[V]) = {
    if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
    if (column2.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")

    val idSet = rows.ids.toSet
    val joins1 = timed("join1",  column.getMany(idSet))
    val rows2 = idSet.intersect(joins1.keySet)
    val joins2 = timed ("join2", column2.getMany(rows2) )
    joins2.keySet map {row => (row, joins1(row), joins2(row))}
  }

  def getRows = rows.ids

}


