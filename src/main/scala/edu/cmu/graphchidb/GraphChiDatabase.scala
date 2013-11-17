package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder, VertexIdTranslate}
import java.io.{FileOutputStream, File}
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{QueryCallback, VertexQuery}
import edu.cmu.graphchidb.Util.async
import java.nio.ByteBuffer
import edu.cmu.graphchi.datablocks.{BytesToValueConverter, BooleanConverter}
import edu.cmu.graphchidb.queries.QueryResult
import java.{util, lang}
import edu.cmu.graphchidb.queries.internal.QueryResultContainer
import java.util.{Date, Collections}
import edu.cmu.graphchidb.storage.inmemory.EdgeBuffer
import edu.cmu.graphchi.shards.{PointerUtil, QueryShard}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.text.SimpleDateFormat
import java.util.concurrent.locks.ReadWriteLock
import scala.actors.threadpool.locks.ReentrantReadWriteLock
import edu.cmu.graphchi.util.Sorting

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

  def intervalContaining(dst: Long) = {
    val firstTry = intervals((dst / vertexIdTranslate.getVertexIntervalLength).toInt)
    if (firstTry.contains(dst)) {
      Some(firstTry)
    } else {
      println("Full interval scan...")
      intervals.find(_.contains(dst))
    }
  }

  var initialized = false

  val debugFile = new FileOutputStream(new File(baseFilename + ".debug.txt"))
  val format = new java.text.SimpleDateFormat("dd-MM-yyyy HH:mm:ss")

  /* Debug log */
  def log(msg: String) = {
    val str = format.format(new Date()) + "\t" + msg + "\n"
    debugFile.write(str.getBytes())
    debugFile.flush()
  }

  def timed[R](blockName: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    log(blockName + " " +  (t1 - t0) / 1000000.0 + "ms")
    result
  }


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
    def bufferFor(src:Long) = {
      val firstTry = (src / vertexIdTranslate.getVertexIntervalLength).toInt
      if (buffers(firstTry).interval.contains(src)) {    // TODO: make smarter
        buffers(firstTry).buffer
      } else {
        buffers.find(_.interval.contains(src)).get.buffer
      }
    }

    val bufferLock = new ReentrantReadWriteLock()
    val persistentShardLock = new ReentrantReadWriteLock()

    def addEdge(src: Long, dst:Long, values: Any*) : Unit = {
      // TODO: Handle if value outside of intervals
      bufferLock.writeLock().lock()
      try {
        bufferFor(src).addEdge(src, dst, values:_*)
      } finally {
        bufferLock.writeLock().unlock()
      }
    }

    def bufferedEdges = buffers.map(_.buffer.numEdges).sum

    def mergeBuffers() = {
      if (bufferedEdges > 0) {
        timed("merge shard %d".format(shardIdx), {

          val edgeColumns = columns(edgeIndexing)
          val edgeSize = edgeEncoderDecoder.edgeSize
          var idx = 0
          val workBuffer = ByteBuffer.allocate(edgeSize)

          val (combinedBufferBuffer, totalEdges) = {
            bufferLock.writeLock().lock()
            try {
              val totalEdges = persistentAdjShard.getNumEdges + buffers.map(_.buffer.numEdges).sum
              log("Shard %d: creating new shards with %d edges".format(shardIdx, totalEdges))

              /* Create edgebuffer containing all edges.
                 TODO: take into account that the persistent shard edges are already sorted -- could merge
                 */
              /*
                 TODO: split shard if too big
               */

              val combinedBufferBuffer = new EdgeBuffer(edgeEncoderDecoder, buffers.map(_.buffer.numEdges).sum.toInt)

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
                  combinedBufferBuffer.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
                  i += 1
                }
              })


              // empty buffers
              init()

              (combinedBufferBuffer, totalEdges)
            } finally {
              bufferLock.writeLock().unlock()
            }
          }

          // Sort buffered edges
          Sorting.sortWithValues(combinedBufferBuffer.srcArray, combinedBufferBuffer.dstArray, combinedBufferBuffer.byteArray, edgeSize)

          persistentShardLock.readLock().lock()
          val persistentBuffer = new EdgeBuffer(edgeEncoderDecoder, persistentAdjShard.getNumEdges.toInt)

          try {
            /* Get edges from persistent shard */
            val edgeIterator = persistentAdjShard.edgeIterator()
            while(edgeIterator.hasNext) {
              edgeIterator.next()
              workBuffer.rewind()
              edgeColumns.foreach(c => c._2.readValueBytes(shardIdx, idx, workBuffer))
              // TODO: write directly to buffer
              persistentBuffer.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
            }
          } finally {
            persistentShardLock.readLock().unlock()
          }
          // Now we can remove persistentshard from memory
          persistentShardLock.writeLock().lock()
          val combinedSrc = new Array[Long](totalEdges.toInt)
          val combinedDst = new Array[Long](totalEdges.toInt)
          val combinedValues = new Array[Byte](totalEdges.toInt * edgeSize)

          try {
            persistentAdjShard = null

            /* Merge */
            Sorting.mergeWithValues(combinedBufferBuffer.srcArray, combinedBufferBuffer.dstArray, combinedBufferBuffer.byteArray,
              persistentBuffer.srcArray, persistentBuffer.dstArray, persistentBuffer.byteArray,
              combinedSrc, combinedDst, combinedValues, edgeSize)


            log("Merge %d edges".format(totalEdges))

            // Write shard
            FastSharder.writeAdjacencyShard(baseFilename, shardIdx, numShards, edgeSize, combinedSrc,
              combinedDst, combinedValues, intervals(shardIdx).getFirstVertex,
              intervals(shardIdx).getLastVertex, true)
            // Initialize newly recreated shard
            persistentAdjShard = new QueryShard(baseFilename, shardIdx, numShards)
          } finally {
            persistentShardLock.writeLock().unlock()
          }
          // TODO: consider synchronization
          // Write data columns, i.e replace the column shard with new data
          (0 until columns(edgeIndexing).size).foreach(columnIdx => {
            val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
            EdgeBuffer.projectColumnToBuffer(columnIdx, columnBuffer, edgeEncoderDecoder, combinedValues, totalEdges.toInt)
            columns(edgeIndexing)(columnIdx)._2.recreateWithData(shardIdx, columnBuffer.array())
          })

        }) // timed
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
  val counter = new AtomicLong(0)
  val pendingBufferFlushes = new AtomicInteger(0)

  def addEdge(src: Long, dst: Long, values: Any*) : Unit = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    shardForEdge(src, dst).addEdge(src, dst, values:_*)

    if (counter.incrementAndGet() % 100000 == 0) {
      if (totalBufferedEdges > bufferLimit * 0.9) {
        if (pendingBufferFlushes.get() > 0) {
          if (totalBufferedEdges < bufferLimit) {
            return
          }
        }
        while(pendingBufferFlushes.get() > 0) {
          log("Waiting for pending flush")
          Thread.sleep(50)
        }

        pendingBufferFlushes.incrementAndGet()
        async {
          log("Purging buffers. Currently buffered: %d".format(totalBufferedEdges))
          while (totalBufferedEdges >= bufferLimit * 0.8) {
            val biggestBufferShard = shards.zipWithIndex.maxBy(_._1.bufferedEdges)._1
            biggestBufferShard.mergeBuffers()
          }
          pendingBufferFlushes.decrementAndGet()
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

      shard.persistentShardLock.readLock().lock()
      try {
        shard.persistentAdjShard.queryIn(internalId, result)
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
      /* Look for buffers (in parallel, of course) -- TODO: profile if really a good idea */
      shard.bufferLock.readLock().lock()
      try {
        shard.buffers.par.foreach(
          buf => {
            buf.buffer.findInNeighborsCallback(internalId, result)
          }
        )
      } finally {
        shard.bufferLock.readLock().unlock()
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

          shard.persistentShardLock.readLock().lock()
          try {
            shard.persistentAdjShard.queryOut(javaQueryIds, resultContainer)
          } finally {
            shard.persistentShardLock.readLock().unlock()
          }
          /* Look for buffers */
          shard.bufferLock.readLock().lock()
          try {
            shard.bufferFor(internalId).findOutNeighborsCallback(internalId, resultContainer)
          } finally {
            shard.bufferLock.readLock().unlock()
          }
          Some(resultContainer)
        } catch {
          case e: Exception  => {
            e.printStackTrace()
            None
          }
        }
      })

      log("Out query finished")

      new QueryResult(vertexIndexing, results.flatten.map(r => r.resultsFor(internalId)).reduce(_+_))
    } )
  }


  def queryOutMultiple(javaQueryIds: Set[java.lang.Long])  = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    timed ("query-out-multiple", {
      val resultContainer =  new QueryResultContainer(javaQueryIds)

      // TODO: fix this java-scala long mapping
      shards.par.foreach(shard => {
        try {

          shard.persistentShardLock.readLock().lock()
          try {
            shard.persistentAdjShard.queryOut(javaQueryIds, resultContainer)
          } finally {
            shard.persistentShardLock.readLock().unlock()
          }
          /* Look for buffers */
          shard.bufferLock.readLock().lock()
          try {
            javaQueryIds.par.foreach(internalId =>
              shard.bufferFor(internalId).findOutNeighborsCallback(internalId, resultContainer))
          } finally {
            shard.bufferLock.readLock().unlock()
          }
        } catch {
          case e: Exception  => {
            e.printStackTrace()
          }
        }
      })

      log("Out query finished")

      new QueryResult(vertexIndexing, resultContainer.combinedResults())
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