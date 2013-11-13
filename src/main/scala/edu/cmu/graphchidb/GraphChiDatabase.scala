package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder, VertexIdTranslate}
import java.io.File
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{PointerUtil, VertexQuery, QueryShard}
import scala.collection.parallel.mutable
import java.nio.ByteBuffer
import edu.cmu.graphchi.datablocks.{BytesToValueConverter, BooleanConverter}


object GraphChiDatabaseAdmin {

  def createDatabase(baseFilename: String, numShards: Int) : Boolean= {
    // Temporary code!
    val sharder = new FastSharder[Boolean, Boolean](baseFilename, numShards, new VertexProcessor[Boolean] {
      def receiveVertexValue(vertexId: Long, token: String) = false
    },
      new EdgeProcessor[Boolean] {
        def receiveEdge(from: Long, to: Long, token: String) = false
      },
      new BooleanConverter().asInstanceOf[BytesToValueConverter[Boolean]],
      new BooleanConverter().asInstanceOf[BytesToValueConverter[Boolean]])
    sharder.addEdge(0, 1, null)
    sharder.addEdge(1, 0, null)
    sharder.process()
    true
  }


}


/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String, origNumShards: Int) {
  var numShards = origNumShards

  val vertexIdTranslate = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)))
  var intervals = ChiFilenames.loadIntervals(baseFilename, origNumShards).toIndexedSeq



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

  /* For columns associated with edges */
  val edgeIndexing : DatabaseIndexing = new DatabaseIndexing {
    def shardForIndex(idx: Long) = PointerUtil.decodeShardNum(idx)

    def shardSize(idx: Long) = 1000 // FIXME

    def shards = numShards

    def globalToLocal(idx: Long) = PointerUtil.decodeShardPos(idx)
  }

  var columns = scala.collection.mutable.Map[DatabaseIndexing, Seq[(String, Column[Any])]](
    vertexIndexing -> Seq[(String, Column[Any])](),
    edgeIndexing ->  Seq[(String, Column[Any])]()
  )


  /* Columns */
  def createCategoricalColumn(name: String, values: IndexedSeq[String], indexing: DatabaseIndexing) = {
    val col =  new CategoricalColumn(filePrefix=baseFilename + "_COLUMN_cat_" + name.toLowerCase,
      indexing, values)

    columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
    col
  }

  def createIntegerColumn(name: String, indexing: DatabaseIndexing) = {
    val col = new FileColumn[Int](filePrefix=baseFilename + "_COLUMN_int_" + name.toLowerCase,
      sparse=false, _indexing=indexing, converter = ByteConverters.IntByteConverter)
    columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
    col
  }

  def createMySQLColumn(tableName: String, columnName: String, indexing: DatabaseIndexing) = {
    val col = new MySQLBackedColumn[String](tableName, columnName, indexing, vertexIdTranslate)
    columns(indexing) = columns(indexing) :+ (tableName + "." + columnName, col.asInstanceOf[Column[Any]])
    col
  }

  def column(name: String, indexing: DatabaseIndexing) = columns(indexing).find(_._1 == name)

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


  def edgeEncoderDecoder = {

    val encoderSeq =  columns(edgeIndexing).map(m => (x: Any, bb: ByteBuffer) => m._2.encode(x, bb))
    val decoderSeq =  columns(edgeIndexing).map(m => (bb: ByteBuffer) => m._2.decode(bb))

    val _edgeSize = 8 * 2 + columns(edgeIndexing).map(_._2.elementSize).sum
    val idxRange = (0 until encoderSeq.size)

    new EdgeEncoderDecoder {
      // Encodes an edge and its values to a byte buffer. Note: all values must be present
      def encode(out: ByteBuffer, src: Long, dst: Long, values: Any*) = {
        out.putLong(src)
        out.putLong(dst)
        idxRange.foreach(i => {
           println("Encoding " + i + " = " + values(i) + ", " + encoderSeq(i))
           encoderSeq(i)(values(i), out)
        })
        _edgeSize
      }

      def decode(in: ByteBuffer) = DecodedEdge(in.getLong, in.getLong, decoderSeq.map(dec => dec(in)))

      def edgeSize = _edgeSize
    }
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

/**
 * Encodes edge and values to a byte array. These are used for high-performance
 * inserts.
 */
trait EdgeEncoderDecoder {

  // Encodes an edge and its values to a byte buffer. Note: all values must be present
  def encode(out: ByteBuffer, src: Long, dst: Long, values: Any*) : Int

  def decode(in: ByteBuffer) : DecodedEdge

  def edgeSize: Int
}

case class DecodedEdge(src: Long, dst:Long, values: Seq[Any])