package edu.cmu.graphchi.shards;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.GraphChiEnvironment;
import edu.cmu.graphchi.VertexInterval;
import edu.cmu.graphchi.bits.IncreasingEliasGammaSeq;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.FinishQueryException;
import edu.cmu.graphchi.queries.QueryCallback;
import org.apache.commons.collections.map.LRUMap;
import scala.actors.threadpool.locks.Lock;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Shard query support
 * @author Aapo Kyrola
 */
public class QueryShard {

    private ShardIndex index;
    private int shardNum;
    private int numShards;
    private String fileName;
    private File adjFile;

    private LongBuffer adjBuffer;
    private LongBuffer pointerIdxBuffer;
    private IntBuffer inEdgeStartBuffer;
    private VertexInterval interval;


    private static final Timer inEdgeIndexLookupTimer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "inedge-indexlookup"));

    private final Timer outEdgePhase1Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "outedge-phase1"));
    private final Timer outEdgePhase2Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "outedge-phase2"));
    private final Timer outEdgeLookupTimer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "outedge-lookups"));

    private final Timer inEdgePhase1Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "inedge-phase1"));
    private final Timer inEdgePhase2Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "inedge-phase2"));
    private final Counter cacheMissCounter = GraphChiEnvironment.metrics.counter("querycache-misses");
    private final Counter cacheHitCounter = GraphChiEnvironment.metrics.counter("querycache-hits");

    public static boolean pinIndexToMemory = Integer.parseInt(System.getProperty("queryshard.pinindex", "0")) == 1;

    /* Pinned compressed indices */
    private IncreasingEliasGammaSeq gammaSeqVertices;
    private IncreasingEliasGammaSeq gammaSeqOffs;

    private int numEdges;

    public static int queryCacheSize = Integer.parseInt(System.getProperty("queryshard.cachesize", "0"));
    public static boolean freezeCache = false;

    private Map queryCache =  (queryCacheSize == 0 ? null : Collections.synchronizedMap(new HashMap(queryCacheSize)));

    static {
        System.out.println("Query cache size: " + queryCacheSize);
    }

    public QueryShard(String filename, int shardNum, int numShards) throws IOException {
        this(filename, shardNum, numShards, ChiFilenames.loadIntervals(filename, numShards).get(shardNum));
    }

    public final static int BYTES_PER_EDGE = 8;

    public QueryShard(String fileName, int shardNum, int numShards, VertexInterval interval) throws IOException {
        this.shardNum = shardNum;
        this.numShards = numShards;
        this.fileName = fileName;

        this.interval = interval;

        adjFile = new File(ChiFilenames.getFilenameShardsAdj(fileName, shardNum, numShards));
        numEdges = (int) (adjFile.length() / BYTES_PER_EDGE);

        FileChannel channel = new java.io.RandomAccessFile(adjFile, "rw").getChannel();
        adjBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0,
                adjFile.length()).asLongBuffer();
        channel.close();

        index = (pinIndexToMemory ? null : new ShardIndex(adjFile));
        loadPointers();
        loadInEdgeStartBuffer();
    }

    public boolean isEmpty() {
        return numEdges == 0;
    }

    public long getNumEdges()  {
        return numEdges;
    }

    private Long incacheKey(long vertexId, byte edgeType) {
        return -(vertexId * 16 + edgeType);
    }

    private Long outcachekey(long vertexId, byte edgeType) {
        return (vertexId * 16 + edgeType);
    }



    private void loadInEdgeStartBuffer() throws IOException {
        File inEdgeStartBufferFile = new File(ChiFilenames.getFilenameShardsAdjStartIndices(adjFile.getAbsolutePath()));
        FileChannel inEdgeStartChannel = new java.io.RandomAccessFile(inEdgeStartBufferFile, "r").getChannel();
        inEdgeStartBuffer = inEdgeStartChannel.map(FileChannel.MapMode.READ_ONLY, 0, inEdgeStartBufferFile.length()).asIntBuffer();
        inEdgeStartChannel.close();
    }

    public static long totalPinnedSize = 0;
    void loadPointers() throws IOException {
        File pointerFile = new File(ChiFilenames.getFilenameShardsAdjPointers(adjFile.getAbsolutePath()));
        if (!pinIndexToMemory) {
            FileChannel ptrFileChannel = new java.io.RandomAccessFile(pointerFile, "r").getChannel();
            pointerIdxBuffer = ptrFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pointerFile.length()).asLongBuffer();
            ptrFileChannel.close();
        } else {

            System.out.println("Reading fully: " + pointerFile.getName());
            byte[] data = new byte[(int)pointerFile.length()];

            if (data.length == 0) return;

            FileInputStream fis = new FileInputStream(pointerFile);
            int i = 0;
            while (i < data.length) {
                i += fis.read(data, i, data.length - i);
            }
            fis.close();

            pointerIdxBuffer = ByteBuffer.wrap(data).asLongBuffer();


            long[] vertices = new long[pointerIdxBuffer.capacity() - 1];
            long[] offs = new long[vertices.length];

            for(int j=0; j<vertices.length; j++) {
                long x = pointerIdxBuffer.get(j);
                vertices[j] = VertexIdTranslate.getVertexId(x);
                offs[j] = VertexIdTranslate.getAux(x);

                if (j>0 && vertices[j] == vertices[j-1]) {
                    System.out.println(j + " / " + vertices.length + " equals " + vertices[j]);
                }
                if (j>0 && offs[j] == offs[j-1]) {
                    System.out.println(j + " / " + offs.length + " off equals " + offs[j] + " (" + vertices[j-1] + ", " + vertices[j] + ")");
                }
            }

            boolean extraZero = (offs.length > 1 && offs[1] == offs[0]);
            if (extraZero) {
                vertices = Arrays.copyOfRange(vertices, 1, vertices.length);
                offs = Arrays.copyOfRange(offs, 1, offs.length);
            }

            gammaSeqVertices = new IncreasingEliasGammaSeq(vertices);
            gammaSeqOffs = new IncreasingEliasGammaSeq(offs);

            totalPinnedSize += gammaSeqVertices.sizeInBytes();
            totalPinnedSize += gammaSeqOffs.sizeInBytes();

            System.out.println("Total pinned size " + totalPinnedSize / 1024.0 / 1024.0 + " mb");

            pointerIdxBuffer = null;

        }
    }

    // TODO: do not synchronize but make mirror of the buffer
    public Long find(byte edgeType, long src, long dst) {
        final LongBuffer tmpAdjBuffer = adjBuffer.duplicate();
        final LongBuffer tmpPointerBuffer = (pinIndexToMemory? null : pointerIdxBuffer.duplicate());
        ShardIndex.IndexEntry indexEntry = (pinIndexToMemory ? null : index.lookup(src));
        PointerPair ptr = findIdxAndPos(src, indexEntry, tmpPointerBuffer, new long[2]);
        if (ptr.cur != (-1L)) {
            long nextPtr = ptr.next;
            int n = (int) (nextPtr - ptr.cur);

            long adjOffset = ptr.cur;

            if (n < 32) {
                // linear search
                tmpAdjBuffer.position((int)adjOffset);

                for(int i=0; i<n; i++) {
                    long e = tmpAdjBuffer.get();
                    long v = VertexIdTranslate.getVertexId(e);
                     if (v == dst && VertexIdTranslate.getType(e) == edgeType) {
                        return PointerUtil.encodePointer(shardNum, (int) adjOffset + i);
                    } else if (v > dst) {
                        break;
                    }
                }
             } else {
                // Binary search
                int low = (int)adjOffset;
                int high = low + n;
                while(low <= high) {
                    int idx = ((high + low) / 2);
                    long e = tmpAdjBuffer.get(idx);
                    long v = VertexIdTranslate.getVertexId(e);
                    byte type = VertexIdTranslate.getType(e);
                    if (v == dst && type == edgeType) {
                        return PointerUtil.encodePointer(shardNum, idx);
                    }

                    if (v < dst || (dst == v && type < edgeType)) {
                        low = idx + 1;
                    } else {
                        high = idx - 1;
                    }
                }
            }
        }
        return null;
    }

    public boolean deleteEdge(byte edgeType, long src, long dst) {
        Long ptr = find(edgeType, src, dst);
        if (ptr != null) {
            deleteEdgeAtPtr(ptr);
            return true;
        } else {
            return false;
        }
    }

    private void deleteEdgeAtPtr(Long ptr) {
        int idx = PointerUtil.decodeShardPos(ptr);
        long edge = adjBuffer.get(idx);
        adjBuffer.put(idx, VertexIdTranslate.encodeAsDeleted(VertexIdTranslate.getVertexId(edge),
                VertexIdTranslate.getAux(edge)));

        // TODO: delete columns (vardata) --- maybe delete listeners?
    }

    class DeleteCallBack implements QueryCallback {

        @Override
        public boolean immediateReceive() {
            return false;
        }

        @Override
        public void receiveEdge(long src, long dst, byte edgeType, long dataPtr) {
            throw new IllegalStateException();
        }

        @Override
        public void receiveOutNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Byte> edgeTypes, ArrayList<Long> dataPointers) {
            // A bit wasteful, but not a big deal
            for(long ptr : dataPointers) {
                deleteEdgeAtPtr(ptr);
            }
        }

        @Override
        public void receiveInNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Byte> edgeTypes, ArrayList<Long> dataPointers) {
            // A bit wasteful, but not a big deal
            for(long ptr : dataPointers) {
                deleteEdgeAtPtr(ptr);
            }
        }
    }

    public synchronized void deleteAllEdgesFor(long vertexId, boolean hasIn, boolean hasOut) {
        if (interval.contains(vertexId) && hasIn) {
            queryIn(vertexId, new DeleteCallBack(), (byte)0, true);
        }
        if (hasOut)
            queryOut(vertexId, new DeleteCallBack(), (byte)0, true);
    }


    public void queryOut(List<Long> sortedIds, QueryCallback callback, byte edgeType) {
        queryOut(sortedIds, callback, edgeType, false);
    }

    // TODO: wild-card search
    public void queryOut(List<Long> sortedIds, QueryCallback callback, byte edgeType, boolean ignoreType) {
        if (isEmpty()) return;
        try {
            final Timer.Context _timer1 = outEdgePhase1Timer.time();

            if (queryCacheSize > 0 && !queryCache.isEmpty()) {
                if (callback.immediateReceive()) {
                    ArrayList<Long> misses = null;
                    for(long qid : sortedIds) {
                        Long cacheKey = outcachekey(qid, edgeType);
                        long[] cached = (long[]) queryCache.get(cacheKey);
                        if (cached != null) {
                            for(int j=0; j<cached.length; j++) {
                                long vpacket = cached[j];
                                callback.receiveEdge(qid, VertexIdTranslate.getVertexId(vpacket), edgeType,
                                        PointerUtil.encodePointer(shardNum, (int)VertexIdTranslate.getAux(vpacket)));
                            }
                        } else {
                            if (misses == null) misses = new ArrayList<Long>();
                            misses.add(qid);
                        }
                    }

                    if (misses != null) cacheMissCounter.inc(misses.size());
                    if (misses != null) cacheHitCounter.inc(sortedIds.size() - misses.size());
                    else cacheHitCounter.inc(sortedIds.size());
                    sortedIds = misses;

                    if (sortedIds == null) return;
                } else {
                    System.err.println("Caching without immediatereceive not implemented yet");
                }
            }

            ArrayList<ShardIndex.IndexEntry> indexEntries = new ArrayList<>(sortedIds.size());
            if (!pinIndexToMemory) {
                for(Long a : sortedIds) {
                    indexEntries.add(index.lookup(a));
                }
            }

            _timer1.stop();
            final Timer.Context _timer2 = outEdgePhase2Timer.time();


            final LongBuffer tmpPointerIdxBuffer = (pointerIdxBuffer != null ? pointerIdxBuffer.duplicate() : null);
            final LongBuffer tmpAdjBuffer = adjBuffer.duplicate();

            ShardIndex.IndexEntry entry = null;
            long[] workarr = new long[2];
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = (pinIndexToMemory ? null :  indexEntries.get(qIdx)); // ugly
                long vertexId = sortedIds.get(qIdx);

                if (qIdx > 0) {
                    if(sortedIds.get(qIdx - 1) >= vertexId) {
                        throw new IllegalArgumentException("Query ids have to be sorted!");
                    }
                }

                PointerPair ptr = findIdxAndPos(vertexId, entry, tmpPointerIdxBuffer, workarr);
                long curPtr = ptr.cur;

                if (ptr.cur != (-1L)) {
                    long nextPtr = ptr.next;
                    int n = (int) (nextPtr - curPtr);

                    ArrayList<Long> res = (callback.immediateReceive() ? null : new ArrayList<Long>(n));
                    ArrayList<Long> resPointers = (callback.immediateReceive() ? null :new ArrayList<Long>(n));
                    ArrayList<Byte> resTypes = (callback.immediateReceive() ? null :new ArrayList<Byte>(n));

                    tmpAdjBuffer.position((int)curPtr);

                    long[] cached = (queryCache == null ||  queryCache.size() >= queryCacheSize || freezeCache ? null : new long[n]);
                    int cachek = 0;

                    for(int i=0; i<n; i++) {
                        long e = tmpAdjBuffer.get();
                        byte etype = VertexIdTranslate.getType(e);

                        if (ignoreType || etype == edgeType) {
                            if (!callback.immediateReceive()) {
                                res.add(VertexIdTranslate.getVertexId(e));
                                resPointers.add(PointerUtil.encodePointer(shardNum, (int) curPtr + i));
                                resTypes.add(etype);
                            } else {
                                callback.receiveEdge(vertexId, VertexIdTranslate.getVertexId(e),
                                        etype, PointerUtil.encodePointer(shardNum, (int) curPtr + i));
                                if (cached != null) {
                                    cached[cachek++] = VertexIdTranslate.encodeVertexPacket(edgeType, VertexIdTranslate.getVertexId(e),
                                            curPtr + 1);
                                }
                            }
                        }
                    }
                    if (!callback.immediateReceive()) callback.receiveOutNeighbors(vertexId, res, resTypes, resPointers);
                    if (cached != null ) {
                        if (cachek < n) {
                            cached = Arrays.copyOf(cached, cachek);
                        }
                        queryCache.put(outcachekey(vertexId, edgeType), cached);
                    }
                } else {
                    if (!callback.immediateReceive()) callback.receiveOutNeighbors(vertexId, new ArrayList<Long>(0), new ArrayList<Byte>(0), new ArrayList<Long>(0));
                    if (queryCache != null && queryCacheSize > queryCache.size() && !freezeCache) {
                        queryCache.put(outcachekey(vertexId, edgeType), new long[0]);
                    }
                }
            }
            _timer2.stop();
        } catch (FinishQueryException fqe) {
            // Used for cases when query was early fulfilled
            throw fqe;
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    /** Singleton search */
    public void queryOut(long vertexId, QueryCallback callback, byte edgeType) {
        queryOut(vertexId, callback, edgeType, false);
    }

    public void queryOut(long vertexId, QueryCallback callback, byte edgeType, boolean ignoreType) {
        if (isEmpty()) return;
        try {
            final LongBuffer tmpPointerIdxBuffer = (pointerIdxBuffer != null ? pointerIdxBuffer.duplicate() : null);
            final LongBuffer tmpAdjBuffer = adjBuffer.duplicate();
            boolean immediateReceive = callback.immediateReceive();

            ShardIndex.IndexEntry entry = null;
            entry = (pinIndexToMemory ? null :  index.lookup(vertexId)); // ugly

            PointerPair ptr = findIdxAndPos(vertexId, entry, tmpPointerIdxBuffer, new long[2]);
            long curPtr = ptr.cur;

            if (ptr.cur != (-1L)) {
                long nextPtr = ptr.next;
                int n = (int) (nextPtr - curPtr);

                ArrayList<Long> res = (immediateReceive  ? null : new ArrayList<Long>(n));
                ArrayList<Long> resPointers = (immediateReceive ? null :new ArrayList<Long>(n));
                ArrayList<Byte> resTypes = (immediateReceive ? null :new ArrayList<Byte>(n));

                tmpAdjBuffer.position((int)curPtr);

                for(int i=0; i<n; i++) {
                    long e = tmpAdjBuffer.get();
                    byte etype = VertexIdTranslate.getType(e);

                    if (etype == edgeType || ignoreType) {
                        if (!immediateReceive) {
                            res.add(VertexIdTranslate.getVertexId(e));
                            resPointers.add(PointerUtil.encodePointer(shardNum, (int) curPtr + i));
                            resTypes.add(etype);
                        } else {
                            callback.receiveEdge(vertexId, VertexIdTranslate.getVertexId(e),
                                    etype, PointerUtil.encodePointer(shardNum, (int) curPtr + i));
                        }
                    }
                }
                if (!immediateReceive) callback.receiveOutNeighbors(vertexId, res, resTypes, resPointers);

            } else {
                if (!immediateReceive) callback.receiveOutNeighbors(vertexId, new ArrayList<Long>(0), new ArrayList<Byte>(0), new ArrayList<Long>(0));
            }

        } catch (FinishQueryException fqe) {
            // Used for cases when query was early fulfilled
            throw fqe;
        } catch (Exception err) {
            err.printStackTrace();
            throw new RuntimeException(err);
        }
    }




    class PointerPair {
        long cur;
        long next;

        PointerPair(long cur, long next) {
            this.cur = cur;
            this.next = next;
        }
    }

    /**
     * Returns the vertex idx for given vertex in the pointer file, or -1 if not found.
     */
    private PointerPair findIdxAndPos(long vertexId, ShardIndex.IndexEntry sparseIndexEntry, final LongBuffer tmpBuffer, long[] workarr) {
        if (tmpBuffer != null) {
            assert(sparseIndexEntry.vertex <= vertexId);

            // Not pinned
            if (tmpBuffer.capacity() == 0) return new PointerPair(-1, -1);

            int vertexSeq = sparseIndexEntry.vertexSeq;
            long curvid = sparseIndexEntry.vertex;

            if (sparseIndexEntry.nextEntry == null) {
                // Linear search
                tmpBuffer.position(vertexSeq);
                long ptr = tmpBuffer.get();
                while(curvid <= vertexId) {
                    try {
                        curvid = VertexIdTranslate.getVertexId(ptr);
                        if (curvid == vertexId) {
                            return new PointerPair(VertexIdTranslate.getAux(ptr), VertexIdTranslate.getAux(tmpBuffer.get()));
                        }
                        ptr = tmpBuffer.get();
                    } catch (BufferUnderflowException bufe) {
                        return new PointerPair(-1, -1);
                    }
                }
            } else {
                // Binary search
                int low = sparseIndexEntry.vertexSeq;
                int high = sparseIndexEntry.nextEntry.vertexSeq;
                int n = tmpBuffer.capacity();

                while(low <= high) {
                    int idx = ((high + low) / 2);
                    long ptr = tmpBuffer.get(idx);

                    curvid = VertexIdTranslate.getVertexId(ptr);
                    if (curvid == vertexId) {
                        return new PointerPair(VertexIdTranslate.getAux(ptr), VertexIdTranslate.getAux(tmpBuffer.get(idx + 1)));
                    }
                    if (curvid < vertexId) {
                        low = idx + 1;
                    } else {
                        high = idx - 1;
                    }
                }

            }
            return new PointerPair(-1, -1);
        } else {
            if (gammaSeqVertices == null) {
                // Empty shard
                return new PointerPair(-1, -1);

            }

            // Pinned
            int idx = gammaSeqVertices.getIndex(vertexId);
            if (idx == -1) return new PointerPair(-1, -1);

            gammaSeqOffs.getTwo(idx, workarr);
            return new PointerPair(workarr[0], workarr[1]);

        }
    }

    private long findVertexForOff(long qoff, final LongBuffer tmpBuffer) {
        // Binary search to find the start of the vertex
        if (tmpBuffer != null) {
            // non-pinned

            int n = tmpBuffer.capacity();
            int high = n-1;
            int low = tmpBuffer.position();

            // Check if we are close
            long cur = tmpBuffer.get();
            long curoff = VertexIdTranslate.getAux(cur);

            // TODO
            if (qoff > curoff && qoff - curoff < 100) {
                long last = cur;
                while(curoff <= qoff) {
                    last = cur;
                    cur = tmpBuffer.get();
                    curoff = VertexIdTranslate.getAux(cur);
                }
                return VertexIdTranslate.getVertexId(last);
            }

            if (curoff >= qoff) low = 0;

            while(low <= high) {
                int idx = ((high + low) / 2);
                if (idx == n - 1) idx--;
                tmpBuffer.position(idx);
                long x = tmpBuffer.get();
                long x_next = tmpBuffer.get();
                long off = VertexIdTranslate.getAux(x);
                long off_next = VertexIdTranslate.getAux(x_next);

                if (off_next > qoff && off <= qoff) {
                    tmpBuffer.position(idx);
                    return VertexIdTranslate.getVertexId(x);
                }
                if (off < qoff) {
                    low = idx + 1;
                } else {
                    high = idx - 1;
                }
            }
            throw new RuntimeException("Could not find " + qoff);
        } else {
            // pinned
            int idx = gammaSeqOffs.getIndexOfLowerBound(qoff);
            if (idx == -1) {
                for(int i=0; i<gammaSeqOffs.length(); i++) {
                    long x = gammaSeqOffs.get(i);
                    System.out.println(i + " / " + x + " != " + qoff);
                    if (qoff == x) {
                        System.out.println(" ===> could not find " + qoff + " but found at " + i + " / " + gammaSeqOffs.length());
                    }
                    if (x  > qoff) break;
                }

                throw new RuntimeException("(Gamma-version) Could not find " + qoff);
            }
            return gammaSeqVertices.get(idx);
        }
    }


    public void queryIn(Long queryId, QueryCallback callback, byte edgeType) {
        queryIn(queryId, callback, edgeType, false);
    }

    private void queryIn(Long queryId, QueryCallback callback, byte edgeType, boolean ignoreType) {
        if (queryCache != null && callback.immediateReceive()) {
            long[] cached = (long[]) queryCache.get(incacheKey(queryId, edgeType));
            if (cached != null && callback.immediateReceive()) {
                for(int j=0; j<cached.length; j++) {
                    long ptr = cached[j];
                    callback.receiveEdge(VertexIdTranslate.getVertexId(ptr), queryId, edgeType,
                            PointerUtil.encodePointer(shardNum, (int) VertexIdTranslate.getAux(ptr)));
                }
                // cacheHitCounter.inc();
                return;
            } else {
                cacheMissCounter.inc();
            }
        }

        if (queryId < interval.getFirstVertex() || queryId > interval.getLastVertex()) {
            throw new IllegalArgumentException("Vertex " + queryId + " not part of interval:" + interval);
        }
        final LongBuffer tmpBuffer = adjBuffer.duplicate();

        try {
            /* Step 1: collect adj file offsets for the in-edges */
            final Timer.Context _timer1 = inEdgePhase1Timer.time();
            ArrayList<Integer> offsets = new ArrayList<Integer>();
            int END = (1<<26) - 1;

            final IntBuffer startBufferTmp = inEdgeStartBuffer.duplicate();

            // Binary search to find the start of the vertex
            int n = inEdgeStartBuffer.capacity() / 2;
            int low = 0;
            int high = n-1;
            int off = -1;
            int queryRelative = (int) (queryId - interval.getFirstVertex());
            while(low <= high) {
                int idx = ((high + low) / 2);
                int v = startBufferTmp.get(idx * 2);
                if (v == queryRelative) {
                    off = startBufferTmp.get(idx * 2 + 1);
                    break;
                }
                if (v < queryRelative) {
                    low = idx + 1;
                } else {
                    high = idx - 1;
                }
            }

            if (off == (-1)) {
                if (queryCache != null && queryCacheSize > queryCache.size()) {
                    queryCache.put(incacheKey(queryId, edgeType), new long[0]);
                }
                return;
            }


            while(off != END) {
                tmpBuffer.position(off);
                long edge = tmpBuffer.get();

                if (VertexIdTranslate.getVertexId(edge) != queryId) {
                    System.out.println("Mismatch in edge linkage: " + VertexIdTranslate.getVertexId(edge) + " !=" + queryId);
                    throw new RuntimeException("Mismatch in edge linkage: " + VertexIdTranslate.getVertexId(edge) + " !=" + queryId);
                }
                if (ignoreType || VertexIdTranslate.getType(edge) == edgeType) {
                    offsets.add(off);
                }

                off = (int) VertexIdTranslate.getAux(edge);

                if (off > END) {
                    throw new RuntimeException("Encoding error: " + edge + " --> " + VertexIdTranslate.getVertexId(edge)
                            + " off : " + VertexIdTranslate.getAux(edge));
                }
                if (off != END && (off < 0 || off > tmpBuffer.capacity())) {
                    System.err.println("Illegal off when looking for inedges: " + off + ", capacity:" + tmpBuffer.capacity() + ", shardNum=" + shardNum);
                }
            }
            _timer1.stop();

            /* Step 2: collect the vertex ids that contain the offsets by passing over the pointer data */
            /* Find beginning */


            ArrayList<Long> inNeighbors =   (callback.immediateReceive() ? null : new ArrayList<Long>(offsets.size()));
            ArrayList<Long> inNeighborsPtrs =  (callback.immediateReceive() ? null :new ArrayList<Long>(offsets.size()));
            ArrayList<Byte> edgeTypes =  (callback.immediateReceive() ? null : new ArrayList<Byte>(offsets.size()));

            ArrayList<Long> cached = (queryCache == null || queryCache.size() >= queryCacheSize || freezeCache ? null : new ArrayList<Long>());

            final LongBuffer tmpPointerIdxBuffer = (pinIndexToMemory ? null : pointerIdxBuffer.duplicate());

            Iterator<Integer> offsetIterator = offsets.iterator();
            if (!offsets.isEmpty()) {
                final Timer.Context  _timer2 = inEdgePhase2Timer.time();

                int firstOff = offsets.get(0);
                if (tmpPointerIdxBuffer != null) {
                    final Timer.Context  _timer3 = inEdgeIndexLookupTimer.time();
                    ShardIndex.IndexEntry startIndex = index.lookupByOffset(firstOff * 8);
                    _timer3.stop();
                    tmpPointerIdxBuffer.position(startIndex.vertexSeq);
                }

                while (offsetIterator.hasNext()) {
                    off = offsetIterator.next();
                    long vert = findVertexForOff(off, tmpPointerIdxBuffer);
                    if (!callback.immediateReceive()) {
                        inNeighbors.add(vert);
                        inNeighborsPtrs.add(PointerUtil.encodePointer(shardNum, off));
                        edgeTypes.add(edgeType); // TODO with wild card
                    } else {
                        callback.receiveEdge(vert, queryId, edgeType, PointerUtil.encodePointer(shardNum, off));
                    }
                    if (cached != null) {
                        cached.add(VertexIdTranslate.encodeVertexPacket(edgeType, vert, off));
                    }
                }
                _timer2.stop();

                if (cached != null) {
                    long[] cachedArr = new long[cached.size()];
                    for(int j=0; j<cached.size(); j++) cachedArr[j] = cached.get(j);
                    queryCache.put(incacheKey(queryId, edgeType), cachedArr);
                }
            }
            if (!callback.immediateReceive()) {
                callback.receiveInNeighbors(queryId, inNeighbors, edgeTypes, inNeighborsPtrs);
            }

        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }


    public EdgeIterator edgeIterator() {
        if (!pinIndexToMemory) {
            final LongBuffer iterBuffer = adjBuffer.duplicate();
            iterBuffer.position(0);
            final LongBuffer iterPointerBuffer = pointerIdxBuffer.duplicate();
            iterPointerBuffer.position(0);


            return new EdgeIterator() {
                int idx = (-1);
                long ptr = (iterPointerBuffer.capacity() > 0 ?  iterPointerBuffer.get() : -1);
                long nextPtr =  (iterPointerBuffer.capacity() > 0 ?  iterPointerBuffer.get() : -1);
                long nextOff = VertexIdTranslate.getAux(nextPtr);
                long curSrc = VertexIdTranslate.getVertexId(ptr);
                long curDst;
                byte curType;
                long vertexPacket;


                @Override
                public boolean hasNext() {
                    if (idx < numEdges - 1) {
                        vertexPacket = iterBuffer.get();
                        if (VertexIdTranslate.isEdgeDeleted(vertexPacket)) {
                            next(); // Skip over deleted edges
                            return hasNext();
                        } else {
                            return true;
                        }

                    } else {
                        return false;
                    }
                }

                @Override
                public void next() {
                    idx++;
                    if (idx == nextOff) {
                        curSrc = VertexIdTranslate.getVertexId(nextPtr);
                        nextPtr = iterPointerBuffer.get();
                        nextOff = VertexIdTranslate.getAux(nextPtr);
                    }

                    curDst = VertexIdTranslate.getVertexId(vertexPacket);
                    curType = VertexIdTranslate.getType(vertexPacket);
                }

                @Override
                public long getSrc() {
                    return curSrc;
                }

                @Override
                public long getDst() {
                    return curDst;
                }

                @Override
                public byte getType() {
                    return curType;
                }
            };
        } else {
            if (gammaSeqVertices == null) {
                return new EdgeIterator() {
                    @Override
                    public boolean hasNext() {
                        return false;
                    }

                    @Override
                    public void next() {

                    }

                    @Override
                    public long getSrc() {
                        return 0;
                    }

                    @Override
                    public long getDst() {
                        return 0;
                    }

                    @Override
                    public byte getType() {
                        return 0;
                    }
                };
            }
            final LongBuffer iterBuffer = adjBuffer.duplicate();
            iterBuffer.position(0);
            final Iterator<Long> iterPointerVertices = gammaSeqVertices.iterator();
            final Iterator<Long> iterPointerOffs = gammaSeqOffs.iterator();


            return new EdgeIterator() {
                int idx = (-1);
                long ptr = (iterPointerVertices.hasNext() ?  VertexIdTranslate.encodeVertexPacket((byte)0, iterPointerVertices.next(), iterPointerOffs.next()) : -1);
                long nextPtr =  (iterPointerVertices.hasNext() ?  VertexIdTranslate.encodeVertexPacket((byte)0, iterPointerVertices.next(), iterPointerOffs.next()) : -1);
                long nextOff = VertexIdTranslate.getAux(nextPtr);
                long curSrc = VertexIdTranslate.getVertexId(ptr);
                long curDst;
                byte curType;
                long vertexPacket;


                @Override
                public boolean hasNext() {
                    if (idx < numEdges - 1) {
                        vertexPacket = iterBuffer.get();
                        if (VertexIdTranslate.isEdgeDeleted(vertexPacket)) {
                            next(); // Skip over deleted edges
                            return hasNext();
                        } else {
                            return true;
                        }

                    } else {
                        return false;
                    }
                }

                @Override
                public void next() {
                    idx++;
                    if (idx == nextOff) {
                        curSrc = VertexIdTranslate.getVertexId(nextPtr);
                        if (iterPointerVertices.hasNext()) {
                            nextPtr = VertexIdTranslate.encodeVertexPacket((byte)0, iterPointerVertices.next(), iterPointerOffs.next());
                        } else {
                            //  System.out.println("Warning: edgeIterator at " + idx + " but no more left...");
                        }
                        nextOff = VertexIdTranslate.getAux(nextPtr);
                    }

                    curDst = VertexIdTranslate.getVertexId(vertexPacket);
                    curType = VertexIdTranslate.getType(vertexPacket);
                }

                @Override
                public long getSrc() {
                    return curSrc;
                }

                @Override
                public long getDst() {
                    return curDst;
                }

                @Override
                public byte getType() {
                    return curType;
                }
            };
        }
    }

}
