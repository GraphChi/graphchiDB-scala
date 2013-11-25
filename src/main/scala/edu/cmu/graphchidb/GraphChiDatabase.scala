package edu.cmu.graphchidb

import edu.cmu.graphchi.ChiFilenames
import edu.cmu.graphchi.preprocessing.{EdgeProcessor, VertexProcessor, FastSharder, VertexIdTranslate}
import java.io.{IOException, FileOutputStream, File}
import edu.cmu.graphchi.engine.VertexInterval

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{QueryCallback, VertexQuery}
import edu.cmu.graphchidb.Util.async
import java.nio.{BufferUnderflowException, ByteBuffer}
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

  def createDatabase(baseFilename: String) : Boolean= {

    // Temporary code!
    FastSharder.createEmptyGraph(baseFilename, 256, 1L<<33)
    true
  }


}


/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String,  bufferLimit : Int = 10000000) {
  var numShards = 256

  val vertexIdTranslate = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)))
  var intervals = ChiFilenames.loadIntervals(baseFilename, numShards).toIndexedSeq

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

  // TODO: hardcoded
  val shardSizeLimit = 2000000000L / 256L


  class DiskShard(levelIdx: Int,  _shardId : Int, splitIntervals: Seq[VertexInterval], parentShards: Seq[DiskShard]) {
    val persistentShardLock = new ReentrantReadWriteLock()
    val shardId = _shardId

    val myInterval = splitIntervals(levelIdx)


    var persistentShard = {
      try {
        new QueryShard(baseFilename, shardId, numShards, myInterval)
      } catch {
        case ioe: IOException => {
          // TODO: improve
          FastSharder.createEmptyShard(baseFilename, numShards, shardId)
          new QueryShard(baseFilename, shardId, numShards, myInterval)
        }
      }
    }

    def numEdges = persistentShard.getNumEdges

    def reset : Unit = {
      persistentShard = new QueryShard(baseFilename, shardId, numShards, myInterval)

      if (persistentShard.getNumEdges > shardSizeLimit && !parentShards.isEmpty) {
        persistentShardLock.writeLock().lock()
        try {
          log("Shard %d  /%d too full --> merge upwards".format(_shardId, levelIdx))
          mergeToParents
        } finally {
          persistentShardLock.writeLock().unlock()
        }
      }
    }

    def readIntoBuffer(destInterval: VertexInterval): EdgeBuffer = {
      val edgeSize = edgeEncoderDecoder.edgeSize
      val edgeColumns = columns(edgeIndexing)
      val workBuffer = ByteBuffer.allocate(edgeSize)
      val thisBuffer =  new EdgeBuffer(edgeEncoderDecoder, persistentShard.getNumEdges.toInt / 2, bufferId=(-1))
      val edgeIterator = persistentShard.edgeIterator()
      var i = 0
      while(edgeIterator.hasNext) {
        edgeIterator.next
        if (destInterval.contains(edgeIterator.getDst)) {
          workBuffer.rewind()
          edgeColumns.foreach(c => c._2.readValueBytes(shardId, i, workBuffer))
          thisBuffer.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
        }
        i += 1
      }
      thisBuffer.compact
    }

    def mergeToAndClear(destShards: Seq[DiskShard]) : Unit = {
      var totalMergedEdges = 0
      val edgeSize = edgeEncoderDecoder.edgeSize

      try {
        persistentShardLock.writeLock().lock()

        destShards.foreach( destShard => {
          val myEdges = readIntoBuffer(destShard.myInterval)
          val destEdges = destShard.readIntoBuffer(destShard.myInterval)

          val totalEdges = myEdges.numEdges + destEdges.numEdges
          totalMergedEdges += totalEdges
          val combinedSrc = new Array[Long](totalEdges.toInt)
          val combinedDst = new Array[Long](totalEdges.toInt)
          val combinedValues = new Array[Byte](totalEdges.toInt * edgeSize)


          Sorting.mergeWithValues(myEdges.srcArray, myEdges.dstArray, myEdges.byteArray,
            destEdges.srcArray, destEdges.dstArray, destEdges.byteArray,
            combinedSrc, combinedDst, combinedValues, edgeSize)

          log("Merging %d -> %d (%d edges)".format(shardId, destShard.shardId, totalEdges))

          // Write shard
          destShard.persistentShardLock.writeLock().lock()
          try {
            FastSharder.writeAdjacencyShard(baseFilename, destShard.shardId, numShards, edgeSize, combinedSrc,
              combinedDst, combinedValues, destShard.myInterval.getFirstVertex,
              destShard.myInterval.getLastVertex, true)

            // TODO: consider synchronization
            // Write data columns, i.e replace the column shard with new data
            (0 until columns(edgeIndexing).size).foreach(columnIdx => {
              val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
              EdgeBuffer.projectColumnToBuffer(columnIdx, columnBuffer, edgeEncoderDecoder, combinedValues, totalEdges.toInt)
              columns(edgeIndexing)(columnIdx)._2.recreateWithData(destShard.shardId, columnBuffer.array())
            })
            destShard.reset

          } finally {
            destShard.persistentShardLock.writeLock().unlock()
          }
        })

        // Empty my shard
        FastSharder.createEmptyShard(baseFilename, numShards, shardId)
        (0 until columns(edgeIndexing).size).foreach(columnIdx => {
          columns(edgeIndexing)(columnIdx)._2.recreateWithData(shardId, new Array[Byte](0)) // Empty
        })
        reset
      } finally {
        persistentShardLock.writeLock().unlock()
      }
    }

    def mergeToParents = mergeToAndClear(parentShards)
  }

  case class EdgeBufferAndInterval(buffer: EdgeBuffer, interval: VertexInterval)

  case class BufferRef(bufferShardId: Int, nthBuffer: Int)

  def edgeBufferId(bufferShardId: Int, nthBuffer: Int) = {
    assert(nthBuffer < intervals.size)
    intervals.size * bufferShardId + nthBuffer
  }
  def bufferReference(bufferId: Int) = BufferRef((bufferId / intervals.size), bufferId % intervals.size)


  class BufferShard(bufferShardId: Int, splitIntervals: Seq[VertexInterval]) {
    var buffers = Seq[EdgeBufferAndInterval]()

    val myInterval = splitIntervals(bufferShardId)

    def init() : Unit = {
      buffers = intervals.map(interval => EdgeBufferAndInterval(new EdgeBuffer(edgeEncoderDecoder,
        bufferId=edgeBufferId(bufferShardId, interval.getId)), interval))
    }


    val intervalLength = splitIntervals(0).length()
    /* Buffer if chosen by src (shard is chosen by dst) */
    def bufferFor(src:Long) = {
      val firstTry = (src / intervalLength).toInt
      if (buffers(firstTry).interval.contains(src)) {
        buffers(firstTry).buffer
      } else {
        buffers.find(_.interval.contains(src)).get.buffer
      }
    }
    val bufferLock = new ReentrantReadWriteLock()

    def addEdge(src: Long, dst:Long, values: Any*) : Unit = {
      // TODO: Handle if value outside of intervals
      bufferLock.writeLock().lock()
      try {
        bufferFor(src).addEdge(src, dst, values:_*)
      } finally {
        bufferLock.writeLock().unlock()
      }
    }

    def numEdges = buffers.map(_.buffer.numEdges).sum

    def mergeToAndClear(destShards: Seq[DiskShard]) : Unit = {
      var totalMergedEdges = 0

      val edgeSize = edgeEncoderDecoder.edgeSize

      timed("mergeToAndClear %d".format(bufferShardId), {
        bufferLock.writeLock().lock()
        val oldBuffers = buffers
        val numEdgesToMerge = oldBuffers.map(_.buffer.numEdges).sum

        try {
          init()
        } finally {
          bufferLock.writeLock().unlock()
        }
        try {
          destShards.par.foreach( destShard => {
            // This prevents queries for that shard while buffer is being emptied.
            // TODO: improve
            destShard.persistentShardLock.writeLock().lock()

            try {
              val myEdges = new EdgeBuffer(edgeEncoderDecoder, numEdgesToMerge / 2, bufferId=(-1))
              // Get edges from buffers
              timed("Edges from buffers", {
                oldBuffers.foreach( bufAndInt => {
                  val buffer = bufAndInt.buffer
                  val edgeIterator = buffer.edgeIterator
                  var i = 0
                  val workBuffer = ByteBuffer.allocate(edgeSize)

                  while(edgeIterator.hasNext) {
                    edgeIterator.next()
                    workBuffer.rewind()
                    if (destShard.myInterval.contains(edgeIterator.getDst)) {
                      buffer.readEdgeIntoBuffer(i, workBuffer)
                      // TODO: write directly to buffer
                      myEdges.addEdge(edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
                    }
                    i += 1
                  }
                })
                myEdges.compact
              })

              timed("sortEdges", {
                Sorting.sortWithValues(myEdges.srcArray, myEdges.dstArray, myEdges.byteArray, edgeSize)

              })

              val destEdges =  timed("destShard.readIntoBuffer", {
                destShard.readIntoBuffer(destShard.myInterval)
              })

              val totalEdges = myEdges.numEdges + destEdges.numEdges
              this.synchronized {
                totalMergedEdges +=  myEdges.numEdges
              }
              val combinedSrc = new Array[Long](totalEdges.toInt)
              val combinedDst = new Array[Long](totalEdges.toInt)
              val combinedValues = new Array[Byte](totalEdges.toInt * edgeSize)

              timed("buffermerge-sort", {
                Sorting.mergeWithValues(myEdges.srcArray, myEdges.dstArray, myEdges.byteArray,
                  destEdges.srcArray, destEdges.dstArray, destEdges.byteArray,
                  combinedSrc, combinedDst, combinedValues, edgeSize)
              })

              log("Merging buffer %d -> %d (%d edges)".format(bufferShardId, destShard.shardId, totalEdges))

              // Write shard
              timed("buffermerge-writeshard", {
                try {
                  FastSharder.writeAdjacencyShard(baseFilename, destShard.shardId, numShards, edgeSize, combinedSrc,
                    combinedDst, combinedValues, destShard.myInterval.getFirstVertex,
                    destShard.myInterval.getLastVertex, true)
                } finally {
                  destShard.persistentShardLock.writeLock().unlock()
                }
              })

              timed("buffermerge-reset", {
                destShard.reset
              })

              // TODO: consider synchronization
              // Write data columns, i.e replace the column shard with new data
              (0 until columns(edgeIndexing).size).foreach(columnIdx => {
                val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
                EdgeBuffer.projectColumnToBuffer(columnIdx, columnBuffer, edgeEncoderDecoder, combinedValues, totalEdges.toInt)
                columns(edgeIndexing)(columnIdx)._2.recreateWithData(destShard.shardId, columnBuffer.array())
              })
            } catch {
              case e:Exception => e.printStackTrace()
            }
          }
          )
          if (totalMergedEdges != numEdgesToMerge) {
            throw new IllegalStateException("Mismatch in merging: %d != %d".format(numEdgesToMerge, totalMergedEdges))
          }

        } catch {
          case e : Exception => throw new RuntimeException(e)
        }

      })
    }

  }


  //def commitAllToDisk = shards.foreach(_.mergeBuffers())

  val numBufferShards = 4

  def createShards(numShards: Int, idStart: Int, upperLevel: Seq[DiskShard]) : Seq[DiskShard] = {
    val levelIntervals = VertexInterval.createIntervals(intervals.last.getLastVertex, numShards).toIndexedSeq
    (0 until numShards).map(i => new DiskShard(i, i + idStart, levelIntervals,
      upperLevel.filter(_.myInterval.intersects(levelIntervals(i))))).toIndexedSeq
  }

  // Create a tree of shards... think about more elegant way
  val shardSizes = List(256, 64, 16)
  val shardIdStarts = shardSizes.scan(0)(_+_)
  val shardTree =  {
    (0 until shardSizes.size).foldLeft(Seq[Seq[DiskShard]]())((tree : Seq[Seq[DiskShard]], treeLevel: Int) => {
      tree :+ createShards(shardSizes(treeLevel), shardIdStarts(treeLevel), tree.lastOption.getOrElse(Seq[DiskShard]()))
    })
  }

  val shards = shardTree.flatten.toIndexedSeq


  val bufferIntervals = VertexInterval.createIntervals(intervals.last.getLastVertex, 4)
  val bufferShards = (0 until numBufferShards).map(i => new BufferShard(i, bufferIntervals.toIndexedSeq))


  def initialize() : Unit = {
    bufferShards.foreach(_.init())
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
    def nShards = shards.size
    def globalToLocal(idx: Long) = PointerUtil.decodeShardPos(idx)
  }

  var columns = scala.collection.mutable.Map[DatabaseIndexing, Seq[(String, Column[Any])]](
    vertexIndexing -> Seq[(String, Column[Any])](),
    edgeIndexing ->  Seq[(String, Column[Any])]()
  )

  def columnIndex(col: Column[Any]) = columns(col.indexing).map(_._2).indexOf(col)


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


  val bufferIntervalLength = bufferShards(0).myInterval.length()

  def bufferForEdge(src:Long, dst:Long) : BufferShard = {
    bufferShards((dst / bufferIntervalLength).toInt)
  }

  def totalBufferedEdges = bufferShards.map(_.numEdges).sum

  def addEdge(src: Long, dst: Long, values: Any*) : Unit = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    bufferForEdge(src, dst).addEdge(src, dst, values:_*)

    if (counter.incrementAndGet() % 100000 == 0) {
      if (totalBufferedEdges > bufferLimit * 0.9) {
        if (pendingBufferFlushes.get() > 0) {
          if (totalBufferedEdges < bufferLimit) {
            return
          }
        }
        while(pendingBufferFlushes.get() > 0) {
          log("Waiting for pending flush")
          Thread.sleep(200)
        }

        /* Temporary dirty hack -- run four flushers */
        pendingBufferFlushes.incrementAndGet()
        async {
          val maxBuffer = bufferShards.maxBy(_.numEdges)
          if (maxBuffer.numEdges > bufferLimit / bufferShards.size / 2) {
            val destShards = shardTree.last.filter(_.myInterval.intersects(maxBuffer.myInterval))
            maxBuffer.mergeToAndClear(destShards)
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


  /* Column value lookups */
  def edgeColumnValues[T](column: Column[T], pointers: Set[java.lang.Long]) : Map[java.lang.Long, Option[T]] = {
    val persistentPointers = pointers.filter(ptr => !PointerUtil.isBufferPointer(ptr))
    val bufferPointers = pointers.filter(ptr => PointerUtil.isBufferPointer(ptr))

    val columnIdx = columnIndex(column.asInstanceOf[Column[Any]])
    assert(columnIdx >= 0)
    val persistentResults = column.getMany(persistentPointers)
    val buf = ByteBuffer.allocate(edgeEncoderDecoder.edgeSize)
    val bufferResults = bufferPointers.map(ptr => {
      val bufferId = PointerUtil.decodeBufferNum(ptr)
      val bufferIdx = PointerUtil.decodeBufferPos(ptr)
      val bufferRef = bufferReference(bufferId)
      // NOTE: buffer reference may be invalid!!! TODO

      buf.rewind
      bufferShards(bufferRef.bufferShardId).buffers(bufferRef.nthBuffer).buffer.readEdgeIntoBuffer(bufferIdx, buf)
      // TODO: read ony necessary column

      buf.rewind
      val vals = edgeEncoderDecoder.decode(buf, -1, -1)
      ptr -> Some(vals.values(columnIdx).asInstanceOf[T])
    }).toMap
    println("Retrieved %d buffer results, %d persistent", bufferPointers.size, persistentPointers.size)
    persistentResults ++ bufferResults
  }

  /* Queries */
  def queryIn(internalId: Long) = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")
    timed ("query-in", {

      val result = new QueryResultContainer(Set(internalId))
      val targetShards = shards.filter(_.myInterval.contains(internalId))

      targetShards.par.foreach(shard => {
        shard.persistentShardLock.readLock().lock()
        try {
          try {
            shard.persistentShard.queryIn(internalId, result)
          } catch {
            case e: Exception => e.printStackTrace()
          }
        } finally {
          shard.persistentShardLock.readLock().unlock()
        }
      })

      /* Look for buffers (in parallel, of course) -- TODO: profile if really a good idea */
      bufferShards.filter(_.myInterval.contains(internalId)).foreach( bufferShard => {
        bufferShard.bufferLock.readLock().lock()
        try {
          bufferShard.buffers.par.foreach(
            buf => {
              try {
                buf.buffer.findInNeighborsCallback(internalId, result)
              } catch {
                case e: Exception => e.printStackTrace()
              }
            }
          )
        } finally {
          bufferShard.bufferLock.readLock().unlock()
        }
      })
      new QueryResult(edgeIndexing, result.resultsFor(internalId), this)
    } )
  }


  def queryOut(internalId: Long) = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    timed ("query-out", {
      val res =  queryOutMultiple(Set[java.lang.Long](internalId))
      // Note, change indexing
      res.withIndexing(edgeIndexing)
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
            shard.persistentShard.queryOut(javaQueryIds, resultContainer)
          } finally {
            shard.persistentShardLock.readLock().unlock()
          }

        } catch {
          case e: Exception  => {
            e.printStackTrace()
          }
        }
      })

      bufferShards.par.foreach(bufferShard => {
        /* Look for buffers */
        bufferShard.bufferLock.readLock().lock()
        try {
          javaQueryIds.par.foreach(internalId =>
            bufferShard.bufferFor(internalId).findOutNeighborsCallback(internalId, resultContainer))
        } catch {
          case e: Exception  => {
            e.printStackTrace()
          }
        } finally {
          bufferShard.bufferLock.readLock().unlock()
        }
      })
      log("Out query finished")

      new QueryResult(vertexIndexing, resultContainer.combinedResults(), this)
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