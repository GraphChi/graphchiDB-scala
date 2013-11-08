package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.VertexIdTranslate
import java.io.File
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage.{MySQLBackedColumn, CategoricalColumn, Column}
import edu.cmu.graphchi.queries.{VertexQuery, QueryShard}

/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String, origNumShards: Int) {
  var numShards = origNumShards

  val vertexIdTranslate = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)))
  var intervals = ChiFilenames.loadIntervals(baseFilename, origNumShards).toIndexedSeq

  var columns = Map[String, Column[AnyRef]]()

  /* For columns associated with vertices */
  val vertexIndexing : DatabaseIndexing = new DatabaseIndexing {
    def shards = numShards
    def shardForIndex(idx: Long) =
      intervals.find(_.contains(idx)).getOrElse(throw new IllegalArgumentException("Vertex id not found")).getId

    def shardSize(idx: Long) =
      intervals.find(_.contains(idx)).getOrElse(throw new IllegalArgumentException("Vertex id not found")).length()

    def globalToLocal(idx: Long) = {
      val interval = intervals(shardForIndex(idx))
      idx - interval.getFirstVertex
    }
  }

  /* Columns */
  def createCategoricalColumn(name: String, values: IndexedSeq[String], indexing: DatabaseIndexing) = {
     val col =  new CategoricalColumn(filePrefix=baseFilename + "_COLUMN_" + name.toLowerCase,
          indexing, values)

     columns = columns + (name -> col.asInstanceOf[Column[AnyRef]])
     col
  }

  def createMySQLColumn(tableName: String, columnName: String, indexing: DatabaseIndexing) = {
     val col = new MySQLBackedColumn[String](tableName, columnName, indexing, vertexIdTranslate)
     columns = columns + (tableName + "." + columnName -> col.asInstanceOf[Column[AnyRef]])
     col
  }

  def column(name: String) = columns(name)

  def numVertices = intervals.last.getLastVertex

  var queryEngine = new VertexQuery(baseFilename, numShards)

  def originalToInternalId(vertexId: Long) = vertexIdTranslate.forward(vertexId)
  def internalToOriginalId(vertexId: Long) = vertexIdTranslate.backward(vertexId)


  def timed[R](blockName: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    println( blockName + " " +  (t1 - t0) / 1000000 + "ms")
    result
  }

  def queryIn(internalId: Long) = {
    timed ("query-in", { new QueryResult(vertexIndexing, queryEngine.queryInNeighbors(internalId).toSet) } )
  }

  def queryOut(internalId: Long) = {
      timed ("query-out", {
        new QueryResult(vertexIndexing, queryEngine.queryOutNeighbors(internalId).toSet)
      } )
  }

  class QueryResult(indexing: DatabaseIndexing, rows: Set[java.lang.Long]) {

    // TODO: multijoin
      def join[T](column: Column[T]) = {
          if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
          val joins1 = column.getMany(rows)

         joins1.keySet map {row => (row, joins1(row))}
      }

    def join[T, V](column: Column[T], column2: Column[V]) = {
      if (column.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")
      if (column2.indexing != indexing) throw new RuntimeException("Cannot join results with different indexing!")

      val joins1 = timed("join1",  column.getMany(rows))
      val rows2 = rows.intersect(joins1.keySet)
      val joins2 = timed ("join2", column2.getMany(rows2) )
      joins2.keySet map {row => (row, joins1(row), joins2(row))}
    }

    def getRows = rows

  }
}



trait DatabaseIndexing {

  def shards : Int
  def shardForIndex(idx: Long) : Int
  def shardSize(idx: Long) : Long

  def globalToLocal(idx: Long) : Long


}