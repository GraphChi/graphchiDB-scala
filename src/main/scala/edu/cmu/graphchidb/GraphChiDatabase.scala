package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder, VertexIdTranslate}
import java.io.File
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{QueryCallback, PointerUtil, VertexQuery, QueryShard}
import edu.cmu.graphchidb.Util._
import java.nio.ByteBuffer
import edu.cmu.graphchi.datablocks.{BytesToValueConverter, BooleanConverter}
import edu.cmu.graphchidb.queries.QueryResult
import java.{lang, util}
import edu.cmu.graphchidb.queries.internal.QueryResultContainer
import java.util.Collections


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

  /* Graph shards: persistent adjacency shard + buffered edges */
  class GraphShard(shardIdx: Int) {
     val persistentAdjShard = new QueryShard(baseFilename, shardIdx, numShards)
     // val buffer
  }

  val shards = (0 until numShards).map{i => new GraphShard(i)}

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


  /* Vertex id conversions */
  def originalToInternalId(vertexId: Long) = vertexIdTranslate.forward(vertexId)
  def internalToOriginalId(vertexId: Long) = vertexIdTranslate.backward(vertexId)
  def numVertices = intervals.last.getLastVertex


  /* Queries */
  def queryIn(internalId: Long) = {
    timed ("query-in", {

      val intervalAndIdx = intervals.zipWithIndex.find(_._1.contains(internalId)).get

      val result = new QueryResultContainer(Set(internalId))
      shards(intervalAndIdx._2).persistentAdjShard.queryIn(internalId, result)

      new QueryResult(vertexIndexing, result.resultsFor(internalId))
    } )
  }


  def queryOut(internalId: Long) = {
    timed ("query-out", {

      val queryIds = Set(internalId.asInstanceOf[java.lang.Long])
      val results = shards.par.map(shard => {
        val resultContainer =  new QueryResultContainer(queryIds)
        val javaQueryIds = Collections.singleton(internalId.asInstanceOf[java.lang.Long])   // is there a better way?
        shard.persistentAdjShard.queryOut(javaQueryIds, resultContainer)
        resultContainer
      })

      println("Out query finished")

      new QueryResult(vertexIndexing, results.map(r => r.resultsFor(internalId)).reduce(_+_))
    } )
  }


  /**
   * High-performance reusable object for encoding edges into bytes
   */
  def edgeEncoderDecoder = {
    val encoderSeq =  columns(edgeIndexing).map(m => (x: Any, bb: ByteBuffer) => m._2.encode(x, bb))
    val decoderSeq =  columns(edgeIndexing).map(m => (bb: ByteBuffer) => m._2.decode(bb))

    val _edgeSize = 8 * 2 + columns(edgeIndexing).map(_._2.elementSize).sum
    val idxRange = 0 until encoderSeq.size

    new EdgeEncoderDecoder {
      // Encodes an edge and its values to a byte buffer. Note: all values must be present
      def encode(out: ByteBuffer, src: Long, dst: Long, values: Any*) = {
        if (values.size != idxRange.size) throw new IllegalArgumentException("Number of inputs must match the encoder configuration")
        out.putLong(src)
        out.putLong(dst)
        idxRange.foreach(i => {
           encoderSeq(i)(values(i), out)
        })
        _edgeSize
      }

      def decode(in: ByteBuffer) = DecodedEdge(in.getLong, in.getLong, decoderSeq.map(dec => dec(in)))

      def edgeSize = _edgeSize
    }
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