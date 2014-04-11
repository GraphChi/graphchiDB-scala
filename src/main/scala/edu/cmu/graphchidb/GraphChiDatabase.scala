package edu.cmu.graphchidb

import edu.cmu.graphchi.{GraphChiEnvironment, ChiFilenames, VertexInterval}
import edu.cmu.graphchi.preprocessing.{FastSharder, VertexIdTranslate}
import java.io.{FilenameFilter, IOException, FileOutputStream, File}

import scala.collection.JavaConversions._
import edu.cmu.graphchidb.storage._
import edu.cmu.graphchi.queries.{FinishQueryException, QueryCallback}
import edu.cmu.graphchidb.Util.async
import java.nio.{BufferUnderflowException, ByteBuffer}
import edu.cmu.graphchidb.queries.QueryResult
import java.{util, lang}
import edu.cmu.graphchidb.queries.internal.QueryResultContainer
import java.util.{Random, Date, Collections}
import edu.cmu.graphchidb.storage.inmemory.EdgeBuffer
import edu.cmu.graphchi.shards.{PointerUtil, QueryShard}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.text.SimpleDateFormat
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import scala.actors.threadpool.locks.ReentrantReadWriteLock
import edu.cmu.graphchi.util.Sorting
import edu.cmu.graphchidb.compute._
import sun.reflect.generics.reflectiveObjects.NotImplementedException
import edu.cmu.graphchidb.storage.VarDataColumn
import com.typesafe.config.{ConfigFactory, Config}
import scala.Some
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer

object GraphChiDatabaseAdmin {

  def createDatabase(baseFilename: String, numShards:Int = 256,
                     maxId: Long = 1L<<33,
                     replaceExistingFiles:Boolean=false) : Boolean= {
    val graphName = new File(baseFilename).getName
    val directory = new File(baseFilename).getParentFile
    if (!directory.exists()) directory.mkdir()

    if (!replaceExistingFiles && directory.listFiles(new FilenameFilter {
      def accept(dir: File, name: String) = name.contains(graphName)
    }).length > 0) {
      throw new IllegalStateException("Database already exists in directory: " + directory)
    } else {
      FastSharder.createEmptyGraph(baseFilename, numShards, maxId)
      true
    }
  }

  def createDatabaseIfNotExists(baseFilename: String, numShards:Int = 256,
                                maxId: Long = 1L<<33) : Boolean= {
    try {
      createDatabase(baseFilename, numShards, maxId, replaceExistingFiles=false)
    } catch {
      case ise: IllegalStateException => {true}
    }
  }

}


/**
 * Defines a sharded graphchi database.
 * @author Aapo Kyrola
 */
class GraphChiDatabase(baseFilename: String,  disableDegree : Boolean = false,
                       numShards: Int = 256) {


  lazy val config = ConfigFactory.parseFileAnySyntax(new File("conf/graphchidb.conf"))

  val bufferLimit = config.getLong("graphchidb.max_buffered_edges")

  /* Optimization that is largely redundant nowadays.. */
  val enableVertexShardBits  = false

  println("Buffer limit: " + bufferLimit)


  // Create a tree of shards... think about more elegant way
  val shardSizes = {
    def appendIf(szs:List[Int]) : List[Int] = if (szs.head > 16) appendIf(szs.head / 4 :: szs) else szs
    val list = appendIf(List(numShards)).reverse
    if (list.last % 4 != 0) {
      throw new IllegalArgumentException("Last shardtree level must be dividable by 4")    // FIXME
    } else {
      list
    }
  }
  //val shardSizes = List(numShards)


  // println("============== WARNING: NO LSM TREE (RESEARCH BRANCH) ===============")
  //  println("Shard tree %s ".format(shardSizes))

  val shardIdStarts = shardSizes.scan(0)(_+_)

  val bufferParents = shardSizes.last / 4

  val vertexIdTranslate = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)))
  val intervals = ChiFilenames.loadIntervals(baseFilename, numShards).toIndexedSeq
  val intervalLength = intervals(0).length()
  // Used for shard bit optimization
  val vertexIntervalBy64 = intervals.last.getLastVertex / 64




  def myAssert(condition: Boolean) = {
    if (!condition)   {
      System.err.println("Assertion failed!");
      throw new RuntimeException("Assert failed!");
    }
  }

  /* This array keeps track of the largest vertex id currently present in each interval. Due to the modulo-shuffling scheme,
     the vertex Ids start from the "bottom" of the interval lower bound.
   */

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


  /* Todo: refactor */
  var bufferPurgeListers = List[Unit => Unit]()
  def registerPurgeListener(f: Unit => Unit) = bufferPurgeListers = bufferPurgeListers :+ f
  def invokePurgeListeners() = bufferPurgeListers.foreach(_())


  val diskShardPurgeLock = new Object()

  /* Debug log */
  def log(msg: String, flush:Boolean=true) = {
    val str = format.format(new Date()) + "\t" + msg + "\n"
    debugFile.synchronized {
      debugFile.write(str.getBytes)
      if (flush) debugFile.flush()
    }
  }

  def timed[R](blockName: String, block: => R): R = {
    val t0 = System.nanoTime()
    val result = block
    val t1 = System.nanoTime()
    val tms = (t1 - t0) / 1000000.0
    if (tms > 10) {   // Log only 10 ms
      log(blockName + " " +  tms  + "ms", flush=false)
    }
    result
  }

  // TODO: hardcoded
  def shardSizeLimit = {
    val maxSizeInBytes = 256 * 1024L * 1024L // todo, remove hard coding
    maxSizeInBytes / (8 + edgeEncoderDecoder.edgeSize)
  }

  val durableTransactionLog = config.getBoolean("graphchidb.durabletransactions")


  sealed abstract class Shard

  class DiskShard(levelIdx: Int,  _shardId : Int, splitIntervals: Seq[VertexInterval], parentShards: Seq[DiskShard]) extends Shard {
    val persistentShardLock = new ReentrantReadWriteLock()
    val mergeInProgress = new AtomicBoolean(false)
    val modifyLock = new ReentrantReadWriteLock
    val shardId = _shardId


    val myInterval = splitIntervals(levelIdx)


    var persistentShard = {
      try {
        new QueryShard(baseFilename, shardId, numShards, myInterval, config)
      } catch {
        case ioe: IOException => {
          // TODO: improve
          FastSharder.createEmptyShard(baseFilename, numShards, shardId)
          new QueryShard(baseFilename, shardId, numShards, myInterval, config)
        }
      }
    }

    def numEdges = persistentShard.getNumEdges

    def reset() : Unit = {
      persistentShard = new QueryShard(baseFilename, shardId, numShards, myInterval,  config)
    }


    def find(edgeType: Byte, src: Long, dst: Long) : Option[Long] = {
      persistentShardLock.readLock().lock()
      try {
        val idx = persistentShard.find(edgeType, src, dst)
        idx match {
          case null => None
          case _ => Some(idx)
        }
      } finally {
        persistentShardLock.readLock().unlock()
      }
    }


    def checkSize() : Unit = {

      try {
        if (persistentShard.getNumEdges > shardSizeLimit && !parentShards.isEmpty) {
          // Release lock so reads can continue while we wait for purge.
          // NOTE: there is slight change that this shard becomes even larger.
          diskShardPurgeLock.synchronized {
            if (persistentShard.getNumEdges > shardSizeLimit) {
              log("Shard %d  /%d too full (limit: %d edges) --> merge upwards".format(_shardId, levelIdx, shardSizeLimit))
              mergeToParents()
            }
          }
        }
      } catch {
        case e: Exception => {
          e.printStackTrace()
          throw e }
      } finally {
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
        edgeIterator.next()
        if (destInterval.contains(edgeIterator.getDst)) {
          workBuffer.rewind()

          edgeColumns.foreach(c => c._2.readValueBytes(shardId, i, workBuffer))
          thisBuffer.addEdge(edgeIterator.getType, edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
        }
        i += 1
      }
      thisBuffer.compact
    }

    def mergeToAndClear(destShards: Seq[DiskShard]) : Unit = {
      var totalMergedEdges = 0
      val edgeSize = edgeEncoderDecoder.edgeSize
      val success = mergeInProgress.compareAndSet(false, true)

      println("Starting merge and clear (%d) thread:%s".format(shardId, Thread.currentThread().getName))

      if (!success) {
        println("Warning: merge was in progress already (%d)".format(shardId))
        return
      }
      try {

        modifyLock.readLock().lock()
        persistentShardLock.readLock().lock()
        // Note: not parallel in order to not use too much memory (buffer flush is parallel)
        destShards.foreach( destShard => {


          destShard.checkSize()

          while(!destShard.mergeInProgress.compareAndSet(false, true)) {
            log("Waiting for parent shard to merge....")
            Thread.sleep(200)
          }

          log("Upstream merge %d ==> %d thread:%s".format(shardId, destShard.shardId, Thread.currentThread().getName))


          val myEdges = readIntoBuffer(destShard.myInterval)

          destShard.modifyLock.readLock().lock()
          destShard.persistentShardLock.readLock().lock()
          val destEdges = destShard.readIntoBuffer(destShard.myInterval)
          destShard.persistentShardLock.readLock().unlock()


          val totalEdges = myEdges.numEdges + destEdges.numEdges
          totalMergedEdges += totalEdges
          val combinedSrc = new Array[Long](totalEdges.toInt)
          val combinedDstWithType = new Array[Long](totalEdges.toInt)
          val combinedValues = new Array[Byte](totalEdges.toInt * edgeSize)

          Sorting.mergeWithValues(myEdges.srcArray, myEdges.dstArrayWithType, myEdges.byteArray,
            destEdges.srcArray, destEdges.dstArrayWithType, destEdges.byteArray,
            combinedSrc, combinedDstWithType, combinedValues, edgeSize)

          log("Merging %d -> %d (%d edges)".format(shardId, destShard.shardId, totalEdges))

          // Write shard
          try {
            timed("diskshard-merge,writeshard", {
              FastSharder.writeAdjacencyShard(baseFilename, destShard.shardId, numShards, edgeSize, combinedSrc,
                combinedDstWithType, combinedValues, destShard.myInterval.getFirstVertex,
                destShard.myInterval.getLastVertex, true,  destShard.persistentShardLock.writeLock())

            })
            // TODO: consider synchronization
            // Write data columns, i.e replace the column shard with new data
            (0 until columns(edgeIndexing).size).foreach(columnIdx => {
              val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
              EdgeBuffer.projectColumnToBuffer(columnIdx, columnBuffer, edgeEncoderDecoder, combinedValues, totalEdges.toInt)
              columns(edgeIndexing)(columnIdx)._2.recreateWithData(destShard.shardId, columnBuffer.array())
            })
            destShard.reset()
          } catch {
            case  e:Exception => {
              e.printStackTrace()
              throw e
            }
          } finally {

            destShard.persistentShardLock.writeLock().unlock()
            destShard.modifyLock.readLock().unlock()
            destShard.mergeInProgress.compareAndSet(true, false)
          }
        })

        // Empty my shard
        persistentShardLock.readLock().unlock()

        timed("diskshard-merge,emptymyshards", {
          persistentShardLock.writeLock().lock()
          FastSharder.createEmptyShard(baseFilename, numShards, shardId)
          (0 until columns(edgeIndexing).size).foreach(columnIdx => {
            columns(edgeIndexing)(columnIdx)._2.recreateWithData(shardId, new Array[Byte](0)) // Empty
          })
        })
        reset
      } catch {
        case  e:Exception => {
          e.printStackTrace()
          throw e
        }
      } finally {
        modifyLock.readLock().unlock()
        persistentShardLock.writeLock().unlock()

        mergeInProgress.compareAndSet(true, false)
      }

      // Check if upstream shards are too big  -- not in parallel but in background thread

    }

    def mergeToParents() =   diskShardPurgeLock.synchronized { mergeToAndClear(parentShards) }
  }

  case class EdgeBufferAndInterval(buffer: EdgeBuffer, interval: VertexInterval)

  case class BufferRef(bufferShardId: Int, nthBuffer: Int)

  val buffersPerBufferShard = intervals.size * bufferParents
  def edgeBufferId(bufferShardId: Int, parentBufferIdx: Int, nthBuffer: Int) = {
    myAssert(nthBuffer < buffersPerBufferShard)
    buffersPerBufferShard * bufferShardId + parentBufferIdx * intervals.size + nthBuffer
  }


  private def bufferReference(bufferId: Int) = BufferRef(bufferId / buffersPerBufferShard, bufferId % buffersPerBufferShard)


  class BufferShard(bufferShardId: Int, _myInterval: VertexInterval,
                    parentShards:  Seq[DiskShard]) extends Shard {
    var buffers = IndexedSeq[IndexedSeq[EdgeBufferAndInterval]]()
    var oldBuffers = IndexedSeq[IndexedSeq[EdgeBufferAndInterval]]()   // used in handout
    val myInterval = _myInterval
    val parentIntervals = parentShards.map(_.myInterval).toIndexedSeq
    val parentIntervalLength = parentIntervals.head.length()
    val firstDst = myInterval.getFirstVertex
    myAssert(firstDst == parentIntervals(0).getFirstVertex)

    myAssert(parentShards.size <= bufferParents)
    val intervalLength = intervals.head.length()

    val transactionLogFile = new File(baseFilename + "_buffer%d.log".format(bufferShardId))
    var transactionLogOut = if (durableTransactionLog) { new FileOutputStream(transactionLogFile) } else {null}
    lazy val trEdgeEncoder = edgeEncoderDecoder
    lazy val trBuffer = ByteBuffer.allocate(trEdgeEncoder.edgeSize + 16)

    def init() : Unit = {
      // TODO: Clean up!
      // Two dimensional buffer matrix where we have one buffer for each interval, divided
      // to buffers for each of the parents
      log("Init buffers: %d".format(bufferShardId))
      buffers = parentIntervals.map(parentInterval =>
        intervals.map(interval => EdgeBufferAndInterval(new EdgeBuffer(edgeEncoderDecoder,
          bufferId=edgeBufferId(bufferShardId, parentInterval.getId - parentIntervals.head.getId, interval.getId)), interval)))
      if (oldBuffers.isEmpty) oldBuffers = parentIntervals.map(parentInterval =>
        intervals.map(interval => EdgeBufferAndInterval(new EdgeBuffer(edgeEncoderDecoder,
          bufferId=edgeBufferId(bufferShardId, parentInterval.getId - parentIntervals.head.getId, interval.getId)), interval)))


    }

    // Note, to call these methods the acquirer need to hold the readlock
    def buffersForDstQuery(dst: Long) = {
      val parentIdx = ((dst - firstDst) / parentIntervalLength).toInt
      buffers(parentIdx) ++ oldBuffers(parentIdx)
    }
    def buffersForSrcQuery(src: Long) =
      buffers.map(bufs => bufs((src / intervalLength).toInt)) ++  oldBuffers.map(bufs => bufs((src / intervalLength).toInt))


    /* Buffer if chosen by src (shard is chosen by dst) */
    def bufferFor(src:Long, dst:Long) : EdgeBuffer = {
      val buffersByParent = buffers(((dst - firstDst) / parentIntervalLength).toInt)
      val firstTry = (src / intervalLength).toInt
      buffersByParent(firstTry).buffer
    }

    def oldBufferFor(src:Long, dst:Long) : EdgeBuffer = {
      val buffersByParent = oldBuffers(((dst - firstDst) / parentIntervalLength).toInt)
      val firstTry = (src / intervalLength).toInt
      buffersByParent(firstTry).buffer
    }

    def nthBuffer(nth: Int) = buffers(nth / intervals.size)(nth % intervals.size)

    val bufferLock = new ReentrantReadWriteLock()

    def addEdge(edgeType: Byte, src: Long, dst:Long, values: Any*) : Unit = {
      // TODO: Handle if value outside of intervals
      bufferLock.readLock().lock()
      // Check for delayed edges
      try {
        bufferFor(src, dst).addEdge(edgeType, src, dst, values:_*)
        if (transactionLogOut != null) {
          trBuffer.rewind()
          trBuffer.putLong(src)
          trBuffer.putLong(dst)
          trEdgeEncoder.encode(trBuffer, values:_*)
          transactionLogOut.write(trBuffer.array())
        }
      } finally {
        bufferLock.readLock().unlock()
      }


    }



    /* Returns buffer pointer for given edge, if found */
    def find(edgeType: Byte, src: Long, dst: Long) : Option[Long] = {
      bufferLock.readLock().lock()
      try {
        bufferFor(src, dst).find(edgeType, src, dst).orElse(oldBufferFor(src, dst).find(edgeType, src, dst))
      } finally {
        bufferLock.readLock().unlock()
      }
    }

    def getValue[T](edgeType: Byte, src:Long, dst:Long, columnIdx: Int) : Option[T] = {
      bufferLock.readLock().lock()
      try {
        val buffer = bufferFor(src, dst)
        val ptrOpt  = buffer.find(edgeType, src, dst)
        // Need to check old and new buffer...
        if (ptrOpt.isDefined) {
          val byteBuf = ByteBuffer.allocate(edgeEncoderDecoder.edgeSize)
          buffer.readEdgeIntoBuffer(PointerUtil.decodeBufferPos(ptrOpt.get), byteBuf)
          byteBuf.rewind
          val decoded = edgeEncoderDecoder.decode(byteBuf, src, dst)
          Some(decoded.values(columnIdx).asInstanceOf[T])
        } else {
          val oldBuffer = oldBufferFor(src, dst)
          if (ptrOpt.isDefined) {
            val byteBuf = ByteBuffer.allocate(edgeEncoderDecoder.edgeSize)
            oldBuffer.readEdgeIntoBuffer(PointerUtil.decodeBufferPos(ptrOpt.get), byteBuf)
            byteBuf.rewind
            val decoded = edgeEncoderDecoder.decode(byteBuf, src, dst)
            Some(decoded.values(columnIdx).asInstanceOf[T])
          } else {
            None
          }
        }
      } finally {
        bufferLock.readLock().unlock()
      }
    }

    val flushLock = new Object


    def update[T](edgeType: Byte, src: Long, dst: Long, columnIdx: Int, value: T) : Boolean  = {
      if (find(edgeType, src, dst).isDefined) {
        // TODO: if merge in progress, update the buffers but need to add the change to
        // parent shard being merged as well
        flushLock.synchronized {         // Need flush lock
          bufferLock.readLock().lock()
          try {
            val buffer = bufferFor(src, dst)
            val ptrOpt  = buffer.find(edgeType, src, dst)
            if (ptrOpt.isDefined) {
              buffer.setColumnValue(PointerUtil.decodeBufferPos(ptrOpt.get), columnIdx, value)
              return true
            }  else { return false }
          } finally {
            bufferLock.readLock().unlock()
          }
        }
      } else {
        false
      }
    }


    def deleteAllEdgesForVertex(vertexId: Long, hasIn: Boolean, hasOut: Boolean) = {
      flushLock.synchronized {
        if (myInterval.contains(vertexId) && hasIn) {
          buffersForDstQuery(vertexId).foreach(b => b.buffer.deleteAllEdgesForVertex(vertexId))
        }
        if (hasOut) {
          buffersForSrcQuery(vertexId).foreach(b => b.buffer.deleteAllEdgesForVertex(vertexId))
        }
      }
    }

    def deleteEdge(edgeType: Byte, src: Long, dst: Long) : Boolean = {
      flushLock.synchronized {         // Need flush lock
        if (find(edgeType, src, dst).isDefined) {
          bufferLock.readLock().lock()
          try {
            val buffer = bufferFor(src, dst)
            val ptrOpt  = buffer.find(edgeType, src, dst)
            if (ptrOpt.isDefined) {
              buffer.deleteEdgeAt(PointerUtil.decodeBufferPos(ptrOpt.get))
              return true
            }  else { return false }
          } finally {
            bufferLock.readLock().unlock()
          }

        } else {
          false
        }
      }
    }

    def numEdgesInclDeletions = buffers.map(_.map(b => b.buffer.numEdges + b.buffer.deletedEdges).sum).sum
    def numEdges  = buffers.map(_.map(b => b.buffer.numEdges ).sum).sum



    def hasPendingMerge =  oldBuffers.flatten.map(_.buffer.numEdges).sum > 0

    def mergeToParentsAndClear() : Unit = {
      if (numEdges  == 0) return
      flushLock.synchronized {
        var totalMergedEdges = 0

        val edgeSize = edgeEncoderDecoder.edgeSize

        log("Buffer %d starting merge %s".format(bufferShardId, Thread.currentThread().getName))

        timed("mergeToAndClear %d".format(bufferShardId), {
          bufferLock.writeLock().lock()
          if (transactionLogOut != null) {
            transactionLogFile.delete()
            transactionLogOut.close()
            transactionLogOut = new FileOutputStream(transactionLogFile)
          }
          oldBuffers = buffers

          try {
            init()
          } finally {
            bufferLock.writeLock().unlock()
          }

          val numEdgesToMerge = oldBuffers.flatten.map(_.buffer.numEdges).sum
          val oldBufferLock = new Object

          try {

            parentShards.par.foreach( destShard => {
              try {
                // This prevents queries for that shard while buffer is being emptied.
                // TODO: improve

                // Check that not too big... otherwise need to flush to parent first
                destShard.checkSize()

                val parentIdx = parentShards.indexOf(destShard)
                val parEdges = oldBuffers(parentIdx).map(_.buffer.numEdges).sum
                val myEdges = new EdgeBuffer(edgeEncoderDecoder, parEdges, bufferId=(-1))
                // Get edges from buffers

                var j = 0
                timed("Edges from buffers", {
                  oldBuffers(parentIdx).foreach( bufAndInt => {
                    val buffer = bufAndInt.buffer
                    val edgeIterator = buffer.edgeIterator
                    val workBuffer = ByteBuffer.allocate(edgeSize)
                    var i = 0

                    while(edgeIterator.hasNext) {
                      edgeIterator.next()
                      workBuffer.rewind()
                      buffer.readEdgeIntoBuffer(i, workBuffer)

                      myEdges.addEdge(edgeIterator.getType, edgeIterator.getSrc, edgeIterator.getDst, workBuffer.array())
                      i += 1
                      j += i
                    }
                  })
                  if (myEdges.numEdges != parEdges) throw new IllegalStateException("Mismatch %d != %d, deleted: %d, counters: %d, j:%d".format(
                    myEdges.numEdges,parEdges, oldBuffers(parentIdx).map(_.buffer.deletedEdges).sum,
                    oldBuffers(parentIdx).map(_.buffer.counter).sum, j))
                  myAssert(myEdges.numEdges == parEdges)
                })

                timed("sortEdges", {
                  Sorting.sortWithValues(myEdges.srcArray, myEdges.dstArrayWithType, myEdges.byteArray, edgeSize)
                })

                try {
                  invokePurgeListeners()

                  while(!destShard.mergeInProgress.compareAndSet(false, true)) {
                    Thread.sleep(200)
                  }

                  destShard.modifyLock.readLock().lock()
                  destShard.persistentShardLock.readLock().lock()

                  val destEdges =  timed("destShard.readIntoBuffer", {
                    destShard.readIntoBuffer(destShard.myInterval)
                  })

                  destShard.persistentShardLock.readLock.unlock()

                  val totalEdges = myEdges.numEdges + destEdges.numEdges
                  this.synchronized {
                    totalMergedEdges +=  myEdges.numEdges
                  }
                  val combinedSrc = new Array[Long](totalEdges.toInt)
                  val combinedDstWithType = new Array[Long](totalEdges.toInt)
                  val combinedValues = new Array[Byte](totalEdges.toInt * edgeSize)

                  timed("buffermerge-sort", {
                    Sorting.mergeWithValues(myEdges.srcArray, myEdges.dstArrayWithType, myEdges.byteArray,
                      destEdges.srcArray, destEdges.dstArrayWithType, destEdges.byteArray,
                      combinedSrc, combinedDstWithType, combinedValues, edgeSize)
                  })


                  log("Merging buffer %d -> %d (%d buffered edges, %d from old)".format(bufferShardId, destShard.shardId,
                    myEdges.numEdges, destEdges.numEdges))

                  // Write shard
                  timed("buffermerge-writeshard", {
                    FastSharder.writeAdjacencyShard(baseFilename, destShard.shardId, numShards, edgeSize, combinedSrc,
                      combinedDstWithType, combinedValues, destShard.myInterval.getFirstVertex,
                      destShard.myInterval.getLastVertex, true, destShard.persistentShardLock.writeLock())

                  })


                  // TODO: consider synchronization
                  // Write data columns, i.e replace the column shard with new data
                  timed("buffermerge-createcols", {
                    (0 until columns(edgeIndexing).size).foreach(columnIdx => {
                      val columnBuffer = ByteBuffer.allocate(totalEdges.toInt * edgeEncoderDecoder.columnLength(columnIdx))
                      EdgeBuffer.projectColumnToBuffer(columnIdx, columnBuffer, edgeEncoderDecoder, combinedValues, totalEdges.toInt)
                      columns(edgeIndexing)(columnIdx)._2.recreateWithData(destShard.shardId, columnBuffer.array())
                    })
                  })
                  timed("buffermerge-reset", {
                    destShard.reset()
                    destShard.persistentShardLock.writeLock().unlock()
                    destShard.modifyLock.readLock().unlock()

                  })
                  destShard.mergeInProgress.compareAndSet(true, false)
                  log("Remove from oldBuffers: %d/%d %s".format(bufferShardId, parentIdx, parentIntervals(parentIdx)))
                  // Remove the edges from the buffer since they are now assumed to be in the persistent shard
                  oldBufferLock.synchronized {
                    oldBuffers = oldBuffers.patch(parentIdx, IndexedSeq(intervals.map(interval => EdgeBufferAndInterval(new EdgeBuffer(edgeEncoderDecoder,
                      bufferId=edgeBufferId(bufferShardId, parentIdx, interval.getId)), interval))), 1)
                  }
                } catch {
                  case e:Exception => e.printStackTrace()
                } finally {
                }
              } catch {
                case e:Exception => e.printStackTrace()
              }
            }
            )
            if (totalMergedEdges != numEdgesToMerge) {
              throw new IllegalStateException("Mismatch in merging: %d != %d".format(numEdgesToMerge, totalMergedEdges))
            }

          } catch {
            case e : Exception => {
              e.printStackTrace()
              throw new RuntimeException(e)
            }
          }

        })

        myAssert(oldBuffers.size == buffers.size)

        myAssert(oldBuffers.map(_.map(_.buffer.numEdges).sum).sum == 0)
      }
    }

  }


  //def commitAllToDisk = shards.foreach(_.mergeBuffers())

  val numBufferShards = 4

  def createShards(numShards: Int, idStart: Int, upperLevel: Seq[DiskShard]) : Seq[DiskShard] = {
    val levelIntervals = VertexInterval.createIntervals(intervals.last.getLastVertex, numShards).toIndexedSeq
    (0 until numShards).map(i => new DiskShard(i, i + idStart, levelIntervals,
      upperLevel.filter(_.myInterval.intersects(levelIntervals(i))))).toIndexedSeq
  }


  val shardTree =  {
    (0 until shardSizes.size).foldLeft(Seq[Seq[DiskShard]]())((tree : Seq[Seq[DiskShard]], treeLevel: Int) => {
      tree :+ createShards(shardSizes(treeLevel), shardIdStarts(treeLevel), tree.lastOption.getOrElse(Seq[DiskShard]()))
    })
  }

  val shards = shardTree.flatten.toIndexedSeq


  private val bufferIntervals = VertexInterval.createIntervals(intervals.last.getLastVertex, 4)
  val bufferShards = (0 until numBufferShards).map(i => new BufferShard(i, bufferIntervals(i),
    shardTree.last.filter(s => s.myInterval.intersects(bufferIntervals(i)))))


  private val shutdownHook = new Thread() {
    override def run() = {
      databaseOpen = false
      println("Run shutdown hook...")
      flushAllBuffers()
      println("Finished shutdown hook")

      if (totalBufferedEdges == 0) buffersEmpty = true
      else buffersEmpty = false

      GraphChiEnvironment.reportMetrics()
    }
  }


  val bufferDrainLock = new Object

  def reportBufferStats = {
    log("Edges in buffers=%d".format(totalBufferedEdges))
    bufferShards.zipWithIndex.foreach(tp => log("  Buffer %d, edges=%d".format(tp._2, tp._1.numEdgesInclDeletions)))
  }

  def checkBuffersAndParents(): Unit = {
    if (totalBufferedEdges < 0.4 * bufferLimit) {
      return
    }

    for(i <- 1 to 5) { // Hack
    val perBufferTrigger = (bufferLimit / bufferShards.size  * 0.75).toInt
      val nonPendings = bufferShards.filterNot(_.hasPendingMerge)
      if (nonPendings.nonEmpty) {
        val maxBuffer = nonPendings.maxBy(_.numEdgesInclDeletions)
        try {

          if (math.random < (maxBuffer.numEdges * 1.0 / perBufferTrigger) ) {   // Probabilistic hack
            reportBufferStats
            this.bufferDrainLock.synchronized { // assure only one parallel drain happening at once (to save memory)
              maxBuffer.mergeToParentsAndClear()
            }

          }
        } catch {
          case e : Exception => e.printStackTrace()
        }
      } else {

        val shardsAndSizesToMerge = shards.map(shard => (shard.numEdges, shard)).filter(_._1 > shardSizeLimit * 0.75)
        if (!shardsAndSizesToMerge.isEmpty) {
          val shardToMerge = shardsAndSizesToMerge.head._2
          shardToMerge.mergeToParents()
        }

      }
    }
  }

  val bufferDrainMonitor = new Object
  var databaseOpen : Boolean = true
  var buffersEmpty: Boolean = true

  var autoFilledEdgeColumns : Seq[Column[Any]]  = Seq()
  var autoFilledVertexColumns: Seq[Column[Any]] = Seq()

  def autoFillEdgeValues = (src: Long, dst: Long, edgeType: Byte) => autoFilledEdgeColumns.map(_.autoFillEdge.get(src, dst, edgeType))

  def initialize() : Unit = {
    // Sort columns so that auto-filled columns are last
    columns(edgeIndexing) = columns(edgeIndexing).filter(_._2.autoFillEdge.isEmpty) ++ columns(edgeIndexing).filter(_._2.autoFillEdge.isDefined)

    // Auto filled columns
    autoFilledEdgeColumns = columns(edgeIndexing).filter(_._2.autoFillEdge.isDefined).map(_._2)
    autoFilledVertexColumns = columns(vertexIndexing).filter(_._2.autoFillVertex.isDefined).map(_._2)

    bufferShards.foreach(_.init())

    // Add shutdown hook
    Runtime.getRuntime.addShutdownHook(shutdownHook)

    initialized = true
    log("Start buffer flusher")
    val flusherThread =new Thread(new Runnable {
      def run() {
        while(databaseOpen) {
          val t1 = System.currentTimeMillis()
          val bufferedEdgesBefore = totalBufferedEdges
          bufferDrainMonitor.synchronized {
            try {
              bufferDrainMonitor.wait(100)
            } catch { case ie : InterruptedException => ie.printStackTrace()}
          }
          val t2 = System.currentTimeMillis()
          val edgesPerMillis = (totalBufferedEdges - bufferedEdgesBefore) / (1 + t2 - t1)

          checkBuffersAndParents()

          val bufferedNow = totalBufferedEdges
          if (bufferedNow > 0 && edgesPerMillis > 0) {
            log("Edges per millis = %d, currently buffered = %d".format(edgesPerMillis, bufferedNow))
          }

        }
      }
    })
    flusherThread.setDaemon(true)
    flusherThread.start()

  }

  def close() = {
    Runtime.getRuntime().removeShutdownHook(shutdownHook)
    shutdownHook.run()
  }

  def shardForEdge(src: Long, dst: Long) = {
    // TODO: handle case where the current intervals don't cover the new id

    shards(intervalContaining(dst).get.getId)
  }

  /* For columns associated with vertices */
  val vertexIndexing : DatabaseIndexing = new DatabaseIndexing {
    def nShards = numShards
    def shardForIndex(idx: Long) =
      intervals((idx / intervalLength).toInt).getId
    def shardSize(idx: Int) = scala.math.max(0, 1 + intervalMaxVertexId(idx) - intervals(idx).getFirstVertex)

    def globalToLocal(idx: Long) = {
      val interval = intervals(shardForIndex(idx))
      idx - interval.getFirstVertex
    }


    def localToGlobal(shardIdx: Int, localIdx: Long) = intervals(shardIdx).getFirstVertex + localIdx

    override def allowAutoExpansion: Boolean = true  // Is this the right place?
    override def name = "vertex"
  }

  /* For columns associated with edges */
  val edgeIndexing : DatabaseIndexing = new DatabaseIndexing {
    def shardForIndex(idx: Long) = PointerUtil.decodeShardNum(idx)
    def shardSize(idx: Int) = shards(idx).numEdges
    def nShards = shards.size


    def localToGlobal(shardIdx: Int, idx: Long) = throw new NotImplementedException

    def globalToLocal(idx: Long) = PointerUtil.decodeShardPos(idx)
    override def name = "edge"

  }

  var columns = scala.collection.mutable.Map[DatabaseIndexing, Seq[(String, Column[Any])]](
    vertexIndexing -> Seq[(String, Column[Any])](),
    edgeIndexing ->  Seq[(String, Column[Any])]()
  )



  /* Columns */
  def createCategoricalColumn(name: String, values: IndexedSeq[String], indexing: DatabaseIndexing,
                              temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")
    this.synchronized {
      val col =  new CategoricalColumn(columns(indexing).size, filePrefix=baseFilename + "_COLUMN_cat_" + indexing.name + "_" + name.toLowerCase,
        indexing, values, deleteOnExit = temporary)

      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createFloatColumn(name: String, indexing: DatabaseIndexing, temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[Float](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_float_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter = ByteConverters.FloatByteConverter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createIntegerColumn(name: String, indexing: DatabaseIndexing, temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[Int](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_int_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter = ByteConverters.IntByteConverter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createShortColumn(name: String, indexing: DatabaseIndexing, temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[Short](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_short_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter = ByteConverters.ShortByteConverter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createByteColumn(name: String, indexing: DatabaseIndexing, temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[Byte](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_byte_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter = ByteConverters.ByteByteConverter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createLongColumn(name: String, indexing: DatabaseIndexing, temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[Long](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_long_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter = ByteConverters.LongByteConverter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createCustomTypeColumn[T](name: String, indexing: DatabaseIndexing, converter: ByteConverter[T], temporary: Boolean=false) = {
    if (initialized && indexing.name == "edge") throw new IllegalStateException("Cannot add edge columns after initialization")

    this.synchronized {
      val col = new FileColumn[T](columns(indexing).size, filePrefix=baseFilename + "_COLUMN_custom" + converter.sizeOf + "_" +  indexing.name + "_" + name.toLowerCase,
        sparse=false, _indexing=indexing, converter=converter, deleteOnExit = temporary)
      if (!temporary) columns(indexing) = columns(indexing) :+ (name, col.asInstanceOf[Column[Any]])
      col
    }
  }

  def createVarDataColumn(name: String, indexing: DatabaseIndexing) : VarDataColumn = {
    this.synchronized {
      val col = new VarDataColumn(name, baseFilename, indexing)
      registerPurgeListener(Unit => col.flushBuffer())
      col
    }
  }

  def createMySQLColumn(tableName: String, columnName: String, indexing: DatabaseIndexing) = {
    val col = new MySQLBackedColumn[String](columns(indexing).size, tableName, columnName, indexing, vertexIdTranslate)
    columns(indexing) = columns(indexing) :+ (tableName + "." + columnName, col.asInstanceOf[Column[Any]])
    col
  }

  def column(name: String, indexing: DatabaseIndexing) = {
    val col = columns(indexing).find(_._1 == name)
    if (col.isDefined) {
      Some(col.get._2)
    } else {
      None
    }
  }

  def columnT[T](name: String, indexing: DatabaseIndexing) = {
    column(name, indexing).asInstanceOf[Option[Column[T]]]
  }

  /* Adding edges */
  // TODO: bulk version
  val counter = new AtomicLong(0)


  val bufferIntervalLength = bufferShards(0).myInterval.length()

  def bufferForEdge(src:Long, dst:Long) : BufferShard = {
    bufferShards((dst / bufferIntervalLength).toInt)
  }

  def totalBufferedEdges = bufferShards.map(_.numEdgesInclDeletions).sum


  private def autoFillVertexValue(vertexId: Long) = {
    autoFilledVertexColumns.foreach(c => c.set(vertexId, c.autoFillVertex.get(vertexId)))
  }

  def addEdge(edgeType: Byte, src: Long, dst: Long, values: Any*) : Unit = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    if ((edgeType & 0xf0) != 0) {
      throw new IllegalArgumentException("Only 4 bits allowed for edge type!");
    }

    buffersEmpty = false

    /* Record keeping */
    this.synchronized {
      updateVertexRecords(src)
      updateVertexRecords(dst)


      if (autoFilledVertexColumns.nonEmpty) {
        // Check if vertex was just added
        if (inDegree(dst) + outDegree(dst) == 0) {
          autoFillVertexValue(dst)
        }
        if (inDegree(src) + outDegree(src) == 0) {
          autoFillVertexValue(src)
        }
      }

      incrementInDegree(dst)
      incrementOutDegree(src)


      if (enableVertexShardBits) {
        val curBits = shardBitsCol.get(src).getOrElse(0L)
        val newBits = Util.setBit(curBits, (dst / vertexIntervalBy64).toInt)
        if (curBits != newBits) {
          shardBitsCol.set(src, newBits)
        }
      }

      bufferForEdge(src, dst).addEdge(edgeType, src, dst,  values ++ autoFillEdgeValues(src, dst, edgeType) :_*)
    }

    /* Buffer flushing. TODO: make smarter. */

    if (counter.incrementAndGet() % 100000 == 0) {
      while(totalBufferedEdges > bufferLimit) {
        log("Buffers full, waiting... %d / %d".format(totalBufferedEdges, bufferLimit))
        bufferDrainMonitor.synchronized {
          bufferDrainMonitor.notifyAll()
        }
        Thread.sleep(500)

      }
    }
  }

  def flushAllBuffers() = {
    log("Flushing all buffers...")
    reportBufferStats
    this.bufferDrainLock.synchronized{
      bufferShards.foreach(bufferShard => bufferShard.mergeToParentsAndClear())
    }
  }


  def addEdgeOrigId(edgeType: Byte, src:Long, dst:Long, values: Any*) {
    addEdge(edgeType, originalToInternalId(src), originalToInternalId(dst), values:_*)
  }

  // TODO: remove redundancy with updateEdge
  def findEdgePointer(edgeType: Byte, src: Long, dst: Long, debug:Boolean=false)(updateFunc: Option[Long] => Unit) : Unit= {
    val bufferShard = bufferForEdge(src, dst)
    val bufferOpt = bufferShard.find(edgeType, src, dst)

    if (bufferOpt.isDefined) {
      bufferShard.bufferLock.readLock().lock()
      try {
        updateFunc(bufferOpt)
      } finally {
        bufferShard.bufferLock.readLock().unlock()
      }
    } else {
      // TODO: more scala-like
      var found = false
      // Look first the most recent data, so reverse
      val shardIter = shards.filter(s => s.myInterval.contains(dst)).reverseIterator
      shardIter.foreach(
        s => {
          s.persistentShardLock.readLock().lock()
          try {
            val ptrOpt = s.find(edgeType, src, dst)
            if (ptrOpt.isDefined) {
              updateFunc(ptrOpt)
              found = true
              return
            }
          } finally {
            s.persistentShardLock.readLock().unlock()
          }
        }
      )
      if (!found) {
        updateFunc(None)
      }
    }
  }

  /**
   * Updates edge value for given column. Returns true if operation succeeds (edge is found).
   * @param src
   * @param dst
   * @param column
   * @param newValue
   * @tparam T
   * @return
   */
  def updateEdge[T](edgeType: Byte, src: Long, dst: Long, column: Column[T], newValue: T) : Boolean = {
    /* First buffers */
    val bufferShard = bufferForEdge(src, dst)
    if (!bufferShard.update(edgeType, src, dst, column.columnId, newValue)) {
      /* And then persistent shard */
      val possibleShards = shards.filter(s => s.myInterval.contains(dst)).reverse // Look first the most recent data, so reverse
      possibleShards.foreach( shard => {
        shard.persistentShardLock.readLock().lock()
        try {
          val idx = shard.persistentShard.find(edgeType, src, dst)
          if (idx != null) {
            try {
              shard.persistentShardLock.readLock().unlock()
              shard.modifyLock.writeLock().lock()
              shard.persistentShardLock.readLock().lock()

              column.set(idx, newValue)
            } finally {
              shard.modifyLock.writeLock.unlock()
            }
            return true
          }
        } finally {
          shard.persistentShardLock.readLock().unlock()

        }
      })
      false
    } else {
      true
    }
  }


  def updateEdgeOrigId[T](edgeType: Byte, src: Long, dst: Long, column: Column[T], newValue: T) : Boolean = {
    updateEdge(edgeType, originalToInternalId(src), originalToInternalId(dst), column, newValue)
  }


  def deleteEdge(edgeType: Byte, src: Long, dst: Long) : Boolean = {
    /* First buffers */

    val bufferShard = bufferForEdge(src, dst)
    if (!bufferShard.deleteEdge(edgeType, src, dst)) {

      /* And then persistent shard */
      val possibleShards = shards.filter(s => s.myInterval.contains(dst)).reverse // Look first the most recent data, so reverse
      possibleShards.foreach( shard => {
        shard.modifyLock.writeLock.lock()
        shard.persistentShardLock.readLock().lock()
        try {
          if (shard.persistentShard.deleteEdge(edgeType, src, dst)) {
            decrementInDegree(dst)
            decrementOutDegree(src)
            return true
          }
        } finally {
          shard.modifyLock.writeLock.unlock()
          shard.persistentShardLock.readLock().unlock()
        }
      })
      false
    } else {
      this.synchronized {
        decrementInDegree(dst)
        decrementOutDegree(src)
      }
      true
    }
  }

  def deleteEdgeOrigId(edgeType: Byte, src: Long, dst: Long) : Boolean = deleteEdge(edgeType, originalToInternalId(src), originalToInternalId(dst))

  def deleteVertex(internalId: Long) : Boolean = {
    this.synchronized {
      val (inDeg, outDeg) =
        (outDegree(internalId), inDegree(internalId))
      val shardbits = shardBits(internalId)

      if (inDeg + outDeg > 0) {

        // Need to pass all shards!
        bufferShards.filter(s => compareShardBitsToInterval(shardbits, s.myInterval)).par.foreach(bufferShard => bufferShard.deleteAllEdgesForVertex(internalId, inDeg > 0, outDeg > 0))

        shards.reverse.par.foreach(shard => {  // Reverse order to maintain same locking direction
          if (compareShardBitsToInterval(shardbits, shard.myInterval)) {
            shard.persistentShardLock.writeLock().lock()
            try {
              shard.modifyLock.writeLock.lock()
              shard.persistentShard.deleteAllEdgesFor(internalId, inDeg > 0, outDeg > 0)
            } finally {
              shard.modifyLock.writeLock().unlock()
              shard.persistentShardLock.writeLock().unlock()
            }
          }
        })

        degreeColumn.set(internalId, 0L)  // Zero degree
        true
      } else {
        false
      }
    }
  }

  def deleteVertexOrigId(vertexId: Long): Boolean = { deleteVertex(originalToInternalId(vertexId)) }


  def setByPointer[T](column: Column[T], ptr: Long, value: T) = {
    if (PointerUtil.isBufferPointer(ptr)) {
      val bufferId = PointerUtil.decodeBufferNum(ptr)
      val bufferIdx = PointerUtil.decodeBufferPos(ptr)
      val bufferRef = bufferReference(bufferId)
      bufferShards(bufferRef.bufferShardId).nthBuffer(bufferRef.nthBuffer).buffer.setColumnValue(bufferIdx, column.columnId, value)
    } else {
      column.set(ptr, value)
    }
  }
  def getByPointer[T](column: Column[T], ptr: Long, buf: ByteBuffer) : Option[T] = {
    myAssert(column.indexing == edgeIndexing)

    if (PointerUtil.isBufferPointer(ptr)) {
      val bufferId = PointerUtil.decodeBufferNum(ptr)
      val bufferIdx = PointerUtil.decodeBufferPos(ptr)
      val bufferRef = bufferReference(bufferId)
      // NOTE: buffer reference may be invalid!!! TODO

      buf.rewind
      bufferShards(bufferRef.bufferShardId).nthBuffer(bufferRef.nthBuffer).buffer.readEdgeIntoBuffer(bufferIdx, buf)
      // TODO: read only necessary column

      buf.rewind
      val vals = edgeEncoderDecoder.decode(buf, -1, -1)
      Some(vals.values(column.columnId).asInstanceOf[T])
    } else {
      column.get(ptr)
    }
  }
  def getByPointer[T](column: Column[T], ptr: Long) : Option[T] = {
    myAssert(column.indexing == edgeIndexing)
    getByPointer(column, ptr, ByteBuffer.allocate(edgeEncoderDecoder.edgeSize))
  }

  def getEdgeValue[T](edgeType: Byte, src: Long, dst: Long, column: Column[T]) : Option[T] = {
    val bufferShard = bufferForEdge(src, dst)
    val bufferOpt = bufferShard.getValue(edgeType, src, dst, column.columnId)
    bufferOpt.orElse( {
      // Look first the most recent data, so reverse
      // TODO: use shardbits?
      val idxOpt = shards.filter(_.myInterval.contains(dst)).reverseIterator.map(_.find(edgeType, src, dst))
        .find(_.isDefined)
      idxOpt.map(idx => column.get(idx.get)).headOption.getOrElse(None)
    })
  }

  def getEdgeValueOrigId[T](edgeType: Byte, src: Long, dst: Long, column: Column[T]) : Option[T] = {
    getEdgeValue(edgeType, originalToInternalId(src), originalToInternalId(dst), column)
  }

  /* Vertex id conversions */
  def originalToInternalId(vertexId: Long) = vertexIdTranslate.forward(vertexId)
  def internalToOriginalId(vertexId: Long) = vertexIdTranslate.backward(vertexId)

  def numVertices = intervals.zip(intervalMaxVertexId).map(z => z._2 - z._1.getFirstVertex + 1).sum
  def numEdges = shards.map(_.numEdges).sum + bufferShards.map(_.numEdges).sum

  /* Column value lookups */
  def edgeColumnValues[T](column: Column[T], pointers: Set[Long]) : Map[Long, Option[T]] = {
    val persistentPointers = pointers.filter(ptr => !PointerUtil.isBufferPointer(ptr))
    val bufferPointers = pointers.filter(ptr => PointerUtil.isBufferPointer(ptr))

    val columnIdx = column.columnId
    myAssert(columnIdx >= 0)
    val persistentResults = column.getMany(persistentPointers)
    val buf = ByteBuffer.allocate(edgeEncoderDecoder.edgeSize)

    val bufferResults = bufferPointers.map(ptr => {
      ptr -> getByPointer(column, ptr, buf)
    }).toMap
    persistentResults ++ bufferResults
  }

  /* Queries */
  def queryIn(internalId: Long, edgeType: Byte) : QueryResult = {
    val result = new QueryResultContainer(Set(internalId))
    queryIn(internalId, edgeType, result)
    new QueryResult(edgeIndexing, result, this)
  }


  def queryIn(internalId: Long, edgeType: Byte, result: QueryCallback) : Unit = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")
    /* Look for buffers (in parallel, of course) -- TODO: profile if really a good idea */

    if (!buffersEmpty) {
      val matchingBuffers = bufferShards.filter(_.myInterval.contains(internalId))

      matchingBuffers.foreach( bufferShard => {
        bufferShard.bufferLock.readLock().lock()
        try {
          bufferShard.buffersForDstQuery(internalId).foreach(
            buf => {
              try {
                buf.buffer.findInNeighborsCallback(internalId, result, edgeType)
              } catch {
                case e: Exception => e.printStackTrace()
              }
            }
          )
        } finally {
          bufferShard.bufferLock.readLock().unlock()
        }
      })
    }

    /* Look for persistent shards */
    val targetShards = shards.filter(shard => shard.myInterval.contains(internalId) && !shard.persistentShard.isEmpty)
    targetShards.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        try {
          shard.persistentShard.queryIn(internalId, result, edgeType)
        } catch {
          case e: Exception => e.printStackTrace()
        }
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })
  }


  def queryOut(internalId: Long, edgeType: Byte) : QueryResult = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")
    val resultContainer =  new QueryResultContainer(Set(internalId))
    timed ("query-out", {
      queryOut(internalId, edgeType, resultContainer)
    } )
    new QueryResult(edgeIndexing, resultContainer, this)
  }


  def queryOut(internalId: Long, edgeType: Byte, callback: QueryCallback, parallel:Boolean=false) : Unit  = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")
    val qshardBits =   shardBits(internalId)

    if (!buffersEmpty) {
      bufferShards.foreach(bufferShard => {
        /* Look for buffers */
        bufferShard.bufferLock.readLock().lock()
        try {
          bufferShard.buffersForSrcQuery(internalId).foreach(buf => {
            if (compareShardBitsToInterval(qshardBits, bufferShard.myInterval)) {
              buf.buffer.findOutNeighborsCallback(internalId, callback, edgeType)
              if (!buf.interval.contains(internalId))
                throw new IllegalStateException("Buffer interval %s did not contain %s".format(buf.interval, internalId))
            } else {
            }})
        } catch {
          case e: Exception  => e.printStackTrace()
        } finally {
          bufferShard.bufferLock.readLock().unlock()
        }
      })
    }

    val filteredShards = shards.filterNot(_.persistentShard.isEmpty).filter(shard => compareShardBitsToInterval(qshardBits, shard.myInterval))


    if (!parallel || filteredShards.size < 4) {
      val filteredShards = shards.filterNot(_.persistentShard.isEmpty).filter(shard => compareShardBitsToInterval(qshardBits, shard.myInterval))
      filteredShards.foreach(shard => {
        try {
          shard.persistentShardLock.readLock().lock()
          try {
            shard.persistentShard.queryOut(internalId, callback, edgeType)
          } finally {
            shard.persistentShardLock.readLock().unlock()
          }
        } catch {
          case e: Exception  =>  e.printStackTrace()
        }
      })
    } else {
      // Parallelized
      filteredShards.par.foreach(shard => {
        try {
          shard.persistentShardLock.readLock().lock()
          try {
            shard.persistentShard.queryOut(internalId, callback, edgeType)
          } finally {
            shard.persistentShardLock.readLock().unlock()
          }
        } catch {
          case e: Exception  =>  e.printStackTrace()
        }
      })
    }
  }




  def queryOutMultiple(_queryIds: Seq[Long], edgeType: Byte, callback: QueryCallback, parallel:Boolean=false) : Unit = {
    if (!initialized) throw new IllegalStateException("You need to initialize first!")

    val queryIds = _queryIds.sorted

    if (enableVertexShardBits && queryIds.size < 10) {
      val idsWithQueryBits = queryIds.map(id => (id, shardBits(id)))

      if (!buffersEmpty) {
        bufferShards.foreach(bufferShard => {
          /* Look for buffers */
          bufferShard.bufferLock.readLock().lock()
          try {
            idsWithQueryBits.foreach { case (internalId: Long, shardBits: Long) =>
              bufferShard.buffersForSrcQuery(internalId).foreach(buf => {
                if (compareShardBitsToInterval(shardBits, bufferShard.myInterval)) {
                  buf.buffer.findOutNeighborsCallback(internalId, callback, edgeType)
                  if (!buf.interval.contains(internalId))
                    throw new IllegalStateException("Buffer interval %s did not contain %s".format(buf.interval, internalId))
                } else {
                }})}
          } catch {
            case e: Exception  => e.printStackTrace()
          } finally {
            bufferShard.bufferLock.readLock().unlock()
          }
        })
      }
      val filteredShards = shards.filterNot(_.persistentShard.isEmpty)

      var finished = false
      def queryshard(shard: DiskShard) = {
        try {
          val matchingIds =  idsWithQueryBits.filter(t => compareShardBitsToInterval(t._2, shard.myInterval)).map(_._1.asInstanceOf[java.lang.Long])

          if (!finished) {

            if (matchingIds.nonEmpty) {
              shard.persistentShardLock.readLock().lock()
              try {
                shard.persistentShard.queryOut(matchingIds, callback, edgeType)
              } finally {
                shard.persistentShardLock.readLock().unlock()
              }
            }
          }
        } catch {
          case fqe: FinishQueryException => finished = true
          case e: Exception  =>  e.printStackTrace()
        }
      }

      if (!parallel) {
        filteredShards.foreach(shard => queryshard(shard))
      } else {
        filteredShards.par.foreach(shard => queryshard(shard))
      }
    } else {
      if (!buffersEmpty) {
        bufferShards.foreach(bufferShard => {
          /* Look for buffers */
          bufferShard.bufferLock.readLock().lock()
          try {
            queryIds.foreach {  internalId =>
              bufferShard.buffersForSrcQuery(internalId).foreach(buf => {
                buf.buffer.findOutNeighborsCallback(internalId, callback, edgeType)
                if (!buf.interval.contains(internalId))
                  throw new IllegalStateException("Buffer interval %s did not contain %s".format(buf.interval, internalId))
              })}
          } catch {
            case e: Exception  => e.printStackTrace()
          } finally {
            bufferShard.bufferLock.readLock().unlock()
          }
        })
      }
      val filteredShards = shards.filterNot(_.persistentShard.isEmpty)
      val ids = queryIds.map(_.asInstanceOf[java.lang.Long])
      var finished = false

      def queryshard2(shard: DiskShard) = {
        shard.persistentShardLock.readLock().lock()
        try {
          if (!finished) {
            shard.persistentShard.queryOut(ids, callback, edgeType)
          }
        } catch {
          case fqe: FinishQueryException => finished = true
          case exception: Exception => exception.printStackTrace()
        } finally {
          shard.persistentShardLock.readLock().unlock()
        }
      }


      if (!parallel) {
        filteredShards.foreach(shard => queryshard2(shard))
      } else {
        filteredShards.par.foreach(shard => queryshard2(shard))
      }

    }

  }


  // Timers
  val timer_out_buffers = GraphChiEnvironment.metrics.timer("queryout-buffers")
  val timer_out_persistent = GraphChiEnvironment.metrics.timer("queryout-persistent")
  val timer_querybits = GraphChiEnvironment.metrics.timer("queryout-querybits")
  val timer_out_total =  GraphChiEnvironment.metrics.timer("queryout-total")
  val timer_out_combine =  GraphChiEnvironment.metrics.timer("queryout-combine")

  // TODO: query needs to acquire ALL locks before doing query -- AVOID OR DETECT DEADLOCKS!
  def queryOutMultiple(queryIds: Set[Long], edgeType: Byte) : QueryResult = {
    val resultContainer =  new QueryResultContainer(queryIds)
    queryOutMultiple(queryIds.toSeq, edgeType, resultContainer)
    new QueryResult(vertexIndexing, resultContainer, this)
  }

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
        try {
          idxRange.foreach(i => {
            encoderSeq(i)(values(i), out)
          })
        } catch {
          case e:Exception => {
            System.err.println("---------------------------")
            System.err.println("Error encoding edge values to bytes. You need to provide edge values in the same order as they we given:")
            columns(edgeIndexing).zipWithIndex.foreach {
              case (colSpec:(String, Column[Any]), i:Int) => {
                System.err.println("  Column [" + i + "]: " + colSpec._1  + " (expected " + columnLengths(i) +
                  " byte-value, converter " + colSpec._2.typeInfo + ", was given [" + values(i) + "])")
              }}
            System.err.println("---------------------------")
            throw e
          }
        }
        _edgeSize
      }

      def decode(buf: ByteBuffer, src: Long, dst: Long) = DecodedEdge(src, dst, decoderSeq.map(dec => dec(buf)))

      def readIthColumn(buf: ByteBuffer, columnIdx: Int, out: ByteBuffer, workArray: Array[Byte]) = {
        buf.position(buf.position() + columnOffsets(columnIdx))
        val l = columnLengths(columnIdx)
        buf.get(workArray, 0, l)
        out.put(workArray, 0, l)
      }


      // Note, buffer position has to be set to a beginning of row
      def setIthColumnInBuffer[T](buf: ByteBuffer, columnIdx: Int, value: T) = {
        buf.position(buf.position() + columnOffsets(columnIdx))
        val l = columnLengths(columnIdx)
        encoderSeq(columnIdx)(value, buf)
      }

      def edgeSize = _edgeSize
      def columnLength(columnIdx: Int) = columnLengths(columnIdx)
    }
  }




  /* Degree management. Hi bytes = in-degree, lo bytes = out-degree */
  val degreeColumn = createLongColumn("degree", vertexIndexing)
  val shardBitsCol = createLongColumn("shardbits", vertexIndexing)

  /* Initialize max vertex id by looking at the degree columns */
  val intervalMaxVertexId =  degreeColumn.blocks.zip(intervals).map {
    case (block, interval) => block.size + interval.getFirstVertex
  }.toSeq.toArray[Long]


  // Manage shard boundaries etc.
  def updateVertexRecords(internalId: Long): Unit = {
    val shardIdx = (internalId / intervals(0).length()).toInt
    if (intervalMaxVertexId(shardIdx) < internalId) { intervalMaxVertexId(shardIdx) = internalId }
  }
  // Premature optimization
  val degreeEncodingBuffer = ByteBuffer.allocate(8)

  def incrementInDegree(internalId: Long) : Unit = if (!disableDegree) {
    degreeColumn.update(internalId, curOpt => {
      val curValue = curOpt.getOrElse(0L)
      Util.setHi(Util.hiBytes(curValue) + 1, curValue) }, degreeEncodingBuffer)
  }

  def incrementOutDegree(internalId: Long) : Unit = { if (!disableDegree) {
    degreeColumn.update(internalId, curOpt => {
      val curValue = curOpt.getOrElse(0L)
      Util.setLo(Util.loBytes(curValue) + 1, curValue) }, degreeEncodingBuffer)
  }

  }

  def decrementInDegree(internalId: Long) : Unit =  if (!disableDegree) {
    degreeColumn.update(internalId, curOpt => {
      val curValue = curOpt.getOrElse(1L)
      Util.setHi(Util.hiBytes(curValue) - 1, curValue) }, degreeEncodingBuffer)
  }

  def decrementOutDegree(internalId: Long) : Unit = if (!disableDegree) {
    degreeColumn.update(internalId, curOpt => {
      val curValue = curOpt.getOrElse(1L)
      Util.setLo(Util.loBytes(curValue) - 1, curValue) }, degreeEncodingBuffer)
  }


  /* Optimization to keep track approximately that in which shards vertex has out-edges.
     Note: these bits are not maintained for deletions and thus a bit set is not a guarantee
     of existence of edges.
   */
  def shardBits(internalId: Long) : Long = {
    if (enableVertexShardBits) {
      shardBitsCol.get(internalId).getOrElse(0L)
    } else {
      0xffffffffffffffffL
    }
  }

  def compareShardBitsToInterval(bits: Long, interval: VertexInterval) : Boolean = {
    val lowIdx = (interval.getFirstVertex / vertexIntervalBy64).toInt
    val hiIdx = (interval.getLastVertex / vertexIntervalBy64).toInt
    var j = lowIdx
    while(j<=hiIdx) {    // Ugly while instead of (lowIdx to hiIdx) for performance
      if (Util.getBit(bits, j)) return true
      j += 1
    }
    false
  }

  def inDegree(internalId: Long) = Util.hiBytes(degreeColumn.get(internalId).getOrElse(0L))
  def outDegree(internalId: Long) = Util.loBytes(degreeColumn.get(internalId).getOrElse(0L))


  def joinValue[T1](col: Column[T1], vertexId: Long, idx: Int, shardId: Int=0, buffer: Option[EdgeBuffer] = None): T1 = {
    (col.indexing match {
      case `vertexIndexing` => col.get(vertexId)
      case `edgeIndexing` => {
        buffer match {
          case None => col.get(PointerUtil.encodePointer(shardId, idx))
          case Some(buffer) => {
            // TODO: make more efficient
            val buf = ByteBuffer.allocate(edgeEncoderDecoder.edgeSize)
            buffer.readEdgeIntoBuffer(idx, buf)
            val vals = edgeEncoderDecoder.decode(buf, -1, -1)
            Some(vals.values(col.columnId).asInstanceOf[T1])
          }
        }
      }
      case _ => throw new UnsupportedOperationException
    }).get
  }

  /** Computational functionality **/
  def sweepInEdgesWithJoin[T1, T2](interval: VertexInterval, maxVertex: Long, col1: Column[T1], col2: Column[T2])(updateFunc: (Long, Long, Byte, T1, T2) => Unit) = {
    val shardsToSweep = shards.filter(shard => shard.myInterval.intersects(interval))

    shardsToSweep.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        val edgeIterator = shard.persistentShard.edgeIterator()
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst) && dst <= maxVertex) {
            val v1 : T1 = joinValue[T1](col1,  edgeIterator.getSrc, edgeIterator.getIdx, shard.shardId)
            val v2 : T2 = joinValue[T2](col2,  edgeIterator.getSrc, edgeIterator.getIdx, shard.shardId)
            updateFunc(src, dst, edgeIterator.getType, v1, v2)
          }
        }
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })

    val bufferToSweep = bufferShards.find(_.myInterval.intersects(interval)).get
    bufferToSweep.bufferLock.readLock().lock()
    try {
      bufferToSweep.buffersForDstQuery(interval.getFirstVertex).foreach(buf => {
        val edgeIterator = buf.buffer.edgeIterator
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst)  && dst <= maxVertex) {  // The latter comparison is bit awkward
          val v1 : T1 = joinValue[T1](col1,  edgeIterator.getSrc, edgeIterator.getIdx, buffer=Some(buf.buffer))
            val v2 : T2 = joinValue[T2](col2,  edgeIterator.getSrc, edgeIterator.getIdx, buffer=Some(buf.buffer))
            updateFunc(src, dst, edgeIterator.getType, v1, v2)
          }
        }
      })
    } finally {
      bufferToSweep.bufferLock.readLock().unlock()
    }
  }

  def sweepInEdgesWithJoin[T1](col1: Column[T1])(updateFunc: (Long, Long, Byte, T1) => Unit) : Unit = {
    intervals.foreach(interval => sweepInEdgesWithJoin(interval, interval.getLastVertex, col1)(updateFunc))
  }

  def sweepInEdgesWithJoin[T1](interval: VertexInterval, maxVertex: Long, col1: Column[T1])(updateFunc: (Long, Long, Byte, T1) => Unit) : Unit = {
    val shardsToSweep = shards.filter(shard => shard.myInterval.intersects(interval))

    shardsToSweep.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        val edgeIterator = shard.persistentShard.edgeIterator()
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst) && dst <= maxVertex) {
            val v1 : T1 = joinValue[T1](col1,  edgeIterator.getSrc, edgeIterator.getIdx, shard.shardId)
            updateFunc(src, dst, edgeIterator.getType, v1)
          }
        }
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })

    val bufferToSweep = bufferShards.find(_.myInterval.intersects(interval)).get
    bufferToSweep.bufferLock.readLock().lock()
    try {
      bufferToSweep.buffersForDstQuery(interval.getFirstVertex).foreach(buf => {
        val edgeIterator = buf.buffer.edgeIterator
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst)  && dst <= maxVertex) {  // The latter comparison is bit awkward
          val v1 : T1 = joinValue[T1](col1,  edgeIterator.getSrc, edgeIterator.getIdx, buffer=Some(buf.buffer))
            updateFunc(src, dst, edgeIterator.getType, v1)
          }
        }
      })
    } finally {
      bufferToSweep.bufferLock.readLock().unlock()
    }
  }

  def sweepInEdgesWithJoinPtr[T1](interval: VertexInterval, maxVertex: Long, col1: Option[Column[T1]])(updateFunc: (Long, Long, Byte, Long) => Unit) = {
    val shardsToSweep = shards.filter(shard => shard.myInterval.intersects(interval))

    shardsToSweep.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        val edgeIterator = shard.persistentShard.edgeIterator()
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst) && dst <= maxVertex) {
            updateFunc(src, dst, edgeIterator.getType, PointerUtil.encodePointer(shard.shardId, edgeIterator.getIdx))
          }
        }
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })

    val bufferToSweep = bufferShards.find(_.myInterval.intersects(interval)).get
    bufferToSweep.bufferLock.readLock().lock()
    try {
      bufferToSweep.buffersForDstQuery(interval.getFirstVertex).foreach(buf => {
        val edgeIterator = buf.buffer.edgeIterator
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst)  && dst <= maxVertex) {  // The latter comparison is bit awkward
            updateFunc(src, dst, edgeIterator.getType, PointerUtil.encodeBufferPointer(buf.buffer.bufferId, edgeIterator.getIdx))
          }
        }
      })
    } finally {
      bufferToSweep.bufferLock.readLock().unlock()
    }
  }

  // Sliding windows
  def sweepOutEdgesWithJoinPtr[T1](interval: VertexInterval, col1: Option[Column[T1]])(updateFunc: (Long, Long, Byte, Long) => Unit) = {
    val shardsToSweep = shards
    shardsToSweep.foreach(shard => {
      try {
        shard.persistentShardLock.readLock().lock()
        try {
          val edgeIterator = shard.persistentShard.edgeIterator(interval.getFirstVertex)
          var finished = false
          while(edgeIterator.hasNext && !finished) {
            edgeIterator.next()
            val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
            if (src <= interval.getLastVertex) {
              updateFunc(src, dst, edgeIterator.getType, PointerUtil.encodePointer(shard.shardId, edgeIterator.getIdx()))
            } else {
              finished = true
            }
          }
        } catch {
          case err : Exception => err.printStackTrace()
        } finally {
          shard.persistentShardLock.readLock().unlock()
        }
      } catch {
        case err : Exception => err.printStackTrace()
      }
    })

    bufferShards.par.foreach(bufferToSweep => {
      bufferToSweep.bufferLock.readLock().lock()
      try {
        (bufferToSweep.buffers ++ bufferToSweep.oldBuffers).flatten.filter(_.interval.intersects(interval)).foreach(buf => {
          val edgeIterator = buf.buffer.edgeIterator
          while(edgeIterator.hasNext) {
            edgeIterator.next()
            val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
            if (interval.contains(src)) {
              updateFunc(src, dst, edgeIterator.getType, PointerUtil.encodeBufferPointer(buf.buffer.bufferId, edgeIterator.getIdx))
            }
          }
        })
      } finally {
        bufferToSweep.bufferLock.readLock().unlock()
      }
    })
  }

  def sweepAllEdges( )(updateFunc: (Long, Long, Byte) => Unit) = {
    val shardsToSweep = shards

    var doFinish = false
    shardsToSweep.par.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        val edgeIterator = shard.persistentShard.edgeIterator()
        while(edgeIterator.hasNext && !doFinish) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          updateFunc(src, dst, edgeIterator.getType)
        }
      } catch {
        case fqe : FinishQueryException => { doFinish = true } // This is fine
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })

    bufferShards.par.foreach( bufferToSweep => {
      bufferToSweep.bufferLock.readLock().lock()
      try {
        bufferToSweep.buffers.flatten.foreach(buf => {
          val edgeIterator = buf.buffer.edgeIterator
          while(edgeIterator.hasNext) {
            edgeIterator.next()
            val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
            updateFunc(src, dst, edgeIterator.getType)
          }
        })
      } finally {
        bufferToSweep.bufferLock.readLock().unlock()
      }
    })
  }




  def sweepInEdges(interval: VertexInterval, maxVertex: Long)(updateFunc: (Long, Long, Byte) => Unit) = {
    val shardsToSweep = shards.filter(shard => shard.myInterval.intersects(interval))

    shardsToSweep.foreach(shard => {
      shard.persistentShardLock.readLock().lock()
      try {
        val edgeIterator = shard.persistentShard.edgeIterator()
        var idx = 0
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst) && dst <= maxVertex) {
            updateFunc(src, dst, edgeIterator.getType)
          }
          idx += 1
        }
      } finally {
        shard.persistentShardLock.readLock().unlock()
      }
    })

    val bufferToSweep = bufferShards.find(_.myInterval.intersects(interval)).get
    bufferToSweep.bufferLock.readLock().lock()
    try {
      bufferToSweep.buffersForDstQuery(interval.getFirstVertex).foreach(buf => {
        val edgeIterator = buf.buffer.edgeIterator
        var idx = 0
        while(edgeIterator.hasNext) {
          edgeIterator.next()
          val (src, dst) = (edgeIterator.getSrc, edgeIterator.getDst)
          if (interval.contains(dst)  && dst <= maxVertex) {  // The latter comparison is bit awkward
            updateFunc(src, dst, edgeIterator.getType)
          }
          idx += 1
        }
      })
    } finally {
      bufferToSweep.bufferLock.readLock().unlock()
    }
  }

  /* Runs vertex-centric computation, similar to GraphChi */
  def runGraphChiComputation[VT, ET](algo: VertexCentricComputation[VT, ET],
                                     numIterations: Int, enableScheduler:Boolean=false) :Unit = {

    val vertexDataColumn = algo.vertexDataColumn
    val edgeDataColumn = algo.edgeDataColumn

    var scheduler = if (enableScheduler) { new BitSetScheduler(vertexIndexing) } else { new Scheduler() {} }
    scheduler.addTaskToAll()

    breakable { for(iter <- 0 until numIterations) {
      val ctx = new GraphChiContext(iter, numIterations, scheduler)

      /* Get main intervals */
      val execIntervals = (0 until vertexIndexing.nShards).map(i => {
        val first = vertexIndexing.localToGlobal(i, 0)
        val last = intervalMaxVertexId(i)
        (first, last)
      })

      // TODO: locking!

      /* Maximum edges a time */
      val maxEdgesPerInterval = config.getLong("graphchi_computation.max_edges_interval")

      algo.beforeIteration(ctx)

      execIntervals.foreach{ case (intervalSt: Long, intervalEn: Long) => {
        var subIntervalSt = intervalSt

        while (subIntervalSt < intervalEn) {
          var subIntervalEn = subIntervalSt - 1
          var intervalEdges = 0L

          val vertices = new ArrayBuffer[GraphChiVertex[VT, ET]](100000)

          /* Initialize vertices */
          while(intervalEdges < maxEdgesPerInterval && subIntervalEn < intervalEn) {

            // TODO: make work better with very sparsely spaced vertex IDs
            subIntervalEn += 1
            if (subIntervalEn % 100000 == 0) {
              println("%d / %d edges: intervalEdges %d".format(subIntervalEn, intervalEn, intervalEdges))
            }

            if (scheduler.isScheduled(subIntervalEn)) {
              val (inc, outc) = (inDegree(subIntervalEn), outDegree(subIntervalEn))
              intervalEdges += inc + outc
              vertices.append(new GraphChiVertex[VT, ET](subIntervalEn, this, vertexDataColumn, edgeDataColumn, inc, outc
              ))
            } else {
              vertices.append(null)
            }
          }

          /* Load edges (parallel sliding windows) */
          // 1. In-edges
          this.sweepInEdgesWithJoinPtr(new VertexInterval(subIntervalSt, subIntervalEn), subIntervalEn, edgeDataColumn
          ) ((src: Long, dst: Long, edgeType: Byte, dataPtr: Long) => {
            val v = vertices((dst - subIntervalSt).toInt)
            try {
              if (v != null) { v.addInEdge(src, dataPtr)}
            } catch {
              case e:Exception => e.printStackTrace()
            }
          }
          )

          // 2. Out-edges (parallel)
          this.sweepOutEdgesWithJoinPtr(new VertexInterval(subIntervalSt, subIntervalEn), edgeDataColumn
          ) ((src: Long, dst: Long, edgeType: Byte, dataPtr: Long) => {
            try {

              val v = vertices((src - subIntervalSt).toInt)
              if (v != null) { v.addOutEdge(dst, dataPtr) }
            } catch {
              case e:Exception => e.printStackTrace()
            }
          })


          /* Execute update functions -- not parallel now */
          if (algo.isParallel) {
            println("Update subinterval (parallel) " + subIntervalSt + " -- " + subIntervalEn)
            vertices.par.foreach( v => if (v != null) { algo.update(v, ctx) } )
          } else {
            println("Update subinterval " + subIntervalSt + " -- " + subIntervalEn)
            vertices.foreach( v => if (v != null) { algo.update(v, ctx) } )
          }
          println("Done...")

          subIntervalSt = subIntervalEn + 1
        }
      }
      }

      algo.afterIteration(ctx)


      if (scheduler.hasNewTasks || !enableScheduler) {
        scheduler = scheduler.swap
      } else {
        println("No new tasks")
        break
      }

    } }
  }


  var activeComputations = Set[Computation]()

  def runIteration(computation: Computation, continuous: Boolean = false) = {
    if (activeComputations.contains(computation)) {
      println("Computation %s was already active!".format(computation))
    } else {
      activeComputations = activeComputations + computation
      async {
        var iter = 0
        try {
          do {
            timed("runiteration_%s_%d".format(computation, iter), {
              intervals.foreach(int => {
                computation.computeForInterval(int, int.getFirstVertex, intervalMaxVertexId(int.getId))
              } )
            })
            iter += 1
          } while (continuous)
        } finally {
          activeComputations = activeComputations - computation
        }
      }
    }
  }
}



trait DatabaseIndexing {
  def nShards : Int
  def name: String
  def shardForIndex(idx: Long) : Int
  def shardSize(shardIdx: Int) : Long
  def globalToLocal(idx: Long) : Long
  def localToGlobal(shardIdx: Int, localIdx: Long) : Long
  def allowAutoExpansion: Boolean = false  // Is this the right place?
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

  // Note, buffer has to be set to a beginning of row
  def setIthColumnInBuffer[T](buf: ByteBuffer, columnIdx: Int, value: T)

  def columnLength(columnIdx: Int) : Int
}

case class DecodedEdge(src: Long, dst:Long, values: Seq[Any])