package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder, VertexIdTranslate}
import java.io.File
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{QueryCallback, VertexQuery}
import edu.cmu.graphchidb.Util._
import java.nio.ByteBuffer
import edu.cmu.graphchi.datablocks.{BytesToValueConverter, BooleanConverter}
import edu.cmu.graphchidb.queries.QueryResult
import java.{util, lang}
import edu.cmu.graphchidb.queries.internal.QueryResultContainer
import java.util.Collections
import edu.cmu.graphchidb.storage.inmemory.EdgeBuffer
import edu.cmu.graphchi.shards.{PointerUtil, QueryShard}
import java.util.concurrent.atomic.AtomicLong

// TODO: refactor: separate database creation and definition from the graphchidatabase class


object GraphChiDatabaseAdmin {

  def createDatabase(baseFilename: String, numShards: Int) : Boolean= {

    // Temporary code!
    FastSharder.createEmptyGraph(baseFilename, numShards, 1L<<33)
    true
  }


}


/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String, origNumShards: Int, bufferLimit : Int = 10000000) {
  var numShards = origNumShards

  val vertexIdTranslate = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)))
  var intervals = ChiFilenames.loadIntervals(baseFilename, origNumShards).toIndexedSeq

  def intervalContaining(dst: Long) = intervals.find(_.contains(dst))

  var initialized = false

  /* Graph shards: persistent adjacency shard + buffered edges */
  class GraphShard(shardIdx: Int) {
    case class EdgeBufferAndInterval(buffer: EdgeBuffer, interval: VertexInterval)

    var persistentAdjShard = new QueryShard(baseFilename, shardIdx, numShards)
    // val buffer
    def numEdges = persistentAdjShard.getNumEdges // ++ buffer.size

    var buffers = Seq[EdgeBufferAndInterval]()

    def init() : Unit = {
      buffers = intervals.map(interval => EdgeBufferAndInterval(new EdgeBuffer(edgeEncoderDecoder), interval))
    }

    /* Buffer if chosen by src (shard is chosen by dst) */
    def bufferFor(src:Long) = buffers.find(_.interval.contains(src)).get.buffer

    def addEdge(src: Long, dst:Long, values: Any*) : Unit = {
      // TODO: Handle if value outside of intervals
      this.synchronized {
        bufferFor(src).addEdge(src, dst, values:_*)
      }
    }

    def bufferedEdges = buffers.map(_.buffer.numEdges).sum

    def mergeBuffers() = {
      if (bufferedEdges > 0) {
        timed("merge shard %d".format(shardIdx), {
          this.synchronized {
            val totalEdges = persistentAdjShard.getNumEdges + buffers.map(_.buffer.numEdges).sum
            println("Shard %d: creating new shards with %d edges".format(shardIdx, totalEdges))

            /* Create edgebuffer containing all edges.
               TODO: take into account that the persistent shard edges are already sorted -- could merge
               */
            /*
               TODO: split shard if too big
             */
            val allEdgesBuffer = new EdgeBuffer(edgeEncoderDecoder, totalEdges.toInt)
            val edgeIterator = persistentAdjShard.edgeIterator()

            val edgeColumns = columns(edgeIndexing)
            val edgeSize = edgeEncoderDecoder.edgeSize
            var idx = 0
            val workBuffer = ByteBuffer.allocate(edgeSize)
            while(edgeIterator.hasNext) {
              edgeIterator.next()
              workBuffer.rewind()
              edgeColumns.foreach(c => c._2.readValueBytes(shardIdx, idx, workBuffer))
              // TODO: write directly to buffer
              allEdgesBuffer.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
            }

            // Now we can remove persistentshard from memory
            persistentAdjShard = null

            // Get edges from buffers
            buffers.map( bufAndInt => {
              val buffer = bufAndInt.buffer
              val edgeIterator = buffer.edgeIterator
              var i = 0
              while(edgeIterator.hasNext) {
                edgeIterator.next()
                workBuffer.rewind()
                buffer.readEdgeIntoBuffer(i, workBuffer)
                // TODO: write directly to buffer
                allEdgesBuffer.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
                i += 1
              }
            })

            assert(allEdgesBuffer.numEdges == totalEdges)
            println("Read %d edges into buffer".format(allEdgesBuffer.numEdges))

            // Write shard
            FastSharder.writeAdjacencyShard(baseFilename, shardIdx, numShards, edgeSize, allEdgesBuffer.srcArray,
              allEdgesBuffer.dstArray, allEdgesBuffer.byteArray, intervals(shardIdx).getFirstVertex, intervals(shardIdx).getLastVertex)

            // empty buffers
            init()

            // Write data columns, i.e replace the column shard with new data
            (0 until columns(edgeIndexing).size).foreach(columnIdx => {
              val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
              allEdgesBuffer.projectColumnToBuffer(columnIdx, columnBuffer)
              columns(edgeIndexing)(columnIdx)._2.recreateWithData(shardIdx, columnBuffer.array())
            })

            // Initialize newly recreated shard
            persistentAdjShard = new QueryShard(baseFilename, shardIdx, numShards)

          } }) // timed
      }
    }
  }


  def totalBufferedEdges = shards.map(_.bufferedEdges).sum

  def commitAllToDisk = shards.foreach(_.mergeBuffers())

  val shards =  (0 until numShards).map{i => new GraphShard(i)}

  def initialize() : Unit = {
    shards.foreach(_.init())
    initialized = true
  }

  def shardForEdge(src: Long, dst: Long) = {
    // TODO: handle case where the current intervals don't cover the new id
    shards(intervalContaining(dst).get.getId)
  }

  /* For columns associated with vertices */
  val vertexIndexing : DatabaseIndexing = new DatabaseIndexing {
    def nShards = numShards
    def shardForIndex(idx: Long) =
      intervals.find(_.contains(idx)).getOrElse(throw new IllegalArgumentException("Vertex id not found")).getId
    def shardSize(idx: Int) =
      intervals(idx).length()

    def globalToLocal(idx: Long) = {
      val interval = intervals(shardForIndex(idx))
      idx - interval.getFirstVertex
    }
  }

  /* For columns associated with edges */
  val edgeIndexing : DatabaseIndexing = new DatabaseIndexing {
    def shardForIndex(idx: Long) = PointerUtil.decodeShardNum(idx)
    def shardSize(idx: Int) = shards(idx).numEdges
    def nShards = numShards
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


  /* Adding edges */
  // TODO: bulk version
  var counter = new AtomicLong(0)

  def addEdge(src: Long, dst: Long, values: Any*) = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    shardForEdge(src, dst).addEdge(src, dst, values:_*)

    if (counter.incrementAndGet() % (bufferLimit / 2) == 0) {
      synchronized {
        println("Purging buffers to half of limit. Currently buffered: %d".format(totalBufferedEdges))
        while (totalBufferedEdges > bufferLimit / 2) {
          val biggestBufferShard = shards.zipWithIndex.maxBy(_._1.bufferedEdges)._1
          biggestBufferShard.mergeBuffers()
        }
      }

    }
  }

  def addEdgeOrigId(src:Long, dst:Long, values: Any*) {
    addEdge(originalToInternalId(src), originalToInternalId(dst), values:_*)
  }


  /* Vertex id conversions */
  def originalToInternalId(vertexId: Long) = vertexIdTranslate.forward(vertexId)
  def internalToOriginalId(vertexId: Long) = vertexIdTranslate.backward(vertexId)
  def numVertices = intervals.last.getLastVertex


  /* Queries */
  def queryIn(internalId: Long) = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")
    timed ("query-in", {
      val intervalAndIdx = intervals.find(_.contains(internalId)).get

      val result = new QueryResultContainer(Set(internalId))
      val shard = shards(intervalAndIdx.getId)

      shard.synchronized {
        shard.persistentAdjShard.queryIn(internalId, result)

        /* Look for buffers (in parallel, of course) -- TODO: profile if really a good idea */
        shard.buffers.par.foreach(
          buf => {
            buf.buffer.findInNeighborsCallback(internalId, result)
          }
        )
      }
      new QueryResult(vertexIndexing, result.resultsFor(internalId))
    } )
  }


  def queryOut(internalId: Long) = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    timed ("query-out", {

      val queryIds = Set(internalId.asInstanceOf[java.lang.Long])
      val results = shards.par.map(shard => {
        try {
          val resultContainer =  new QueryResultContainer(queryIds)
          val javaQueryIds = Collections.singleton(internalId.asInstanceOf[java.lang.Long])   // is there a better way?

          shard.synchronized {
            shard.persistentAdjShard.queryOut(javaQueryIds, resultContainer)

            /* Look for buffers */
            shard.bufferFor(internalId).findOutNeighborsCallback(internalId, resultContainer)
          }
          Some(resultContainer)
        } catch {
          case e: Exception  => {
            e.printStackTrace()
            None
          }
        }
      })

      println("Out query finished")

      new QueryResult(vertexIndexing, results.flatten.map(r => r.resultsFor(internalId)).reduce(_+_))
    } )
  }


  def queryOutMultiple(javaQueryIds: Set[java.lang.Long])  = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    timed ("query-out-multiple", {
      // TODO: fix this java-scala long mapping
      val results = shards.par.map(shard => {
        try {
          val resultContainer =  new QueryResultContainer(javaQueryIds)

          shard.synchronized {
            shard.persistentAdjShard.queryOut(javaQueryIds, resultContainer)

            /* Look for buffers */
            javaQueryIds.par.foreach(internalId =>
              shard.bufferFor(internalId).findOutNeighborsCallback(internalId, resultContainer))
          }
          Some(resultContainer)
        } catch {
          case e: Exception  => {
            e.printStackTrace()
            None
          }
        }
      })

      println("Out query finished")

      new QueryResult(vertexIndexing, results.flatten.map(r => r.combinedResults()).reduce(_+_))
    } )
  }
  def queryOutMultiple(internalIds: Seq[Long]) : QueryResult = queryOutMultiple(internalIds.map(_.asInstanceOf[java.lang.Long]).toSet)

  /**
   * High-performance reusable object for encoding edges into bytes
   */
  def edgeEncoderDecoder = {
    val encoderSeq =  columns(edgeIndexing).map(m => (x: Any, bb: ByteBuffer) => m._2.encode(x, bb))
    val decoderSeq =  columns(edgeIndexing).map(m => (bb: ByteBuffer) => m._2.decode(bb))

    val columnLengths = columns(edgeIndexing).map(_._2.elementSize).toIndexedSeq
    val columnOffsets = columnLengths.scan(0)(_+_).toIndexedSeq
    val _edgeSize = columns(edgeIndexing).map(_._2.elementSize).sum
    val idxRange = 0 until encoderSeq.size

    new EdgeEncoderDecoder {
      // Encodes an edge and its values to a byte buffer. Note: all values must be present
      def encode(out: ByteBuffer, values: Any*) = {
        if (values.size != idxRange.size)
          throw new IllegalArgumentException("Number of inputs must match the encoder configuration: %d != given %d".format(idxRange.size, values.size))
        idxRange.foreach(i => {
          encoderSeq(i)(values(i), out)
        })
        _edgeSize
      }

      def decode(buf: ByteBuffer, src: Long, dst: Long) = DecodedEdge(src, dst, decoderSeq.map(dec => dec(buf)))

      def readIthColumn(buf: ByteBuffer, columnIdx: Int, out: ByteBuffer, workArray: Array[Byte]) = {
        buf.position(buf.position() + columnOffsets(columnIdx))
        val l = columnLengths(columnIdx)
        buf.get(workArray, 0, l)
        out.put(workArray, 0, l)
      }

      def edgeSize = _edgeSize
      def columnLength(columnIdx: Int) = columnLengths(columnIdx)
    }
  }

}



trait DatabaseIndexing {
  def nShards : Int
  def shardForIndex(idx: Long) : Int
  def shardSize(shardIdx: Int) : Long
  def globalToLocal(idx: Long) : Long
}

/**
 * Encodes edge values to a byte array. These are used for high-performance
 * inserts.
 */
trait EdgeEncoderDecoder {

  // Encodes an edge and its values to a byte buffer. Note: all values must be present
  def encode(out: ByteBuffer, values: Any*) : Int

  def decode(buf: ByteBuffer, src: Long, dst: Long) : DecodedEdge

  def edgeSize: Int

  // For making fast projections. Writes ith column to out
  def readIthColumn(buf: ByteBuffer, columnIdx: Int, out: ByteBuffer, workArray: Array[Byte])

  def columnLength(columnIdx: Int) : Int
}

case class DecodedEdge(src: Long, dst:Long, values: Seq[Any])