package edu.cmu.graphchi.shards;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Timer;
import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.GraphChiEnvironment;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.QueryCallback;


import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

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

    private final Timer inEdgePhase1Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "inedge-phase1"));
    private final Timer inEdgePhase2Timer = GraphChiEnvironment.metrics.timer(name(QueryShard.class, "inedge-phase2"));

    private int numEdges;

    public QueryShard(String filename, int shardNum, int numShards) throws IOException {
        this(filename, shardNum, numShards, ChiFilenames.loadIntervals(filename, numShards).get(shardNum));
    }

    public QueryShard(String fileName, int shardNum, int numShards, VertexInterval interval) throws IOException {
        this.shardNum = shardNum;
        this.numShards = numShards;
        this.fileName = fileName;

        this.interval = interval;

        adjFile = new File(ChiFilenames.getFilenameShardsAdj(fileName, shardNum, numShards));
        numEdges = (int) (adjFile.length() / 8);

        adjBuffer = new java.io.RandomAccessFile(adjFile, "r").getChannel().map(FileChannel.MapMode.READ_ONLY, 0,
                    adjFile.length()).asLongBuffer();

        index = new ShardIndex(adjFile);
        loadPointers();
        loadInEdgeStartBuffer();
    }


    public long getNumEdges()  {
        return numEdges;
    }

    private void loadInEdgeStartBuffer() throws IOException {
        File inEdgeStartBufferFile = new File(ChiFilenames.getFilenameShardsAdjStartIndices(adjFile.getAbsolutePath()));
        FileChannel inEdgeStartChannel = new java.io.RandomAccessFile(inEdgeStartBufferFile, "r").getChannel();
        inEdgeStartBuffer = inEdgeStartChannel.map(FileChannel.MapMode.READ_ONLY, 0, inEdgeStartBufferFile.length()).asIntBuffer();
        inEdgeStartChannel.close();
    }

    void loadPointers() throws IOException {
        File pointerFile = new File(ChiFilenames.getFilenameShardsAdjPointers(adjFile.getAbsolutePath()));
        FileChannel ptrFileChannel = new java.io.RandomAccessFile(pointerFile, "r").getChannel();
        pointerIdxBuffer = ptrFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pointerFile.length()).asLongBuffer();
        ptrFileChannel.close();
    }



    public synchronized void queryOut(Collection<Long> queryIds, QueryCallback callback) {
        try {
             /* Sort the ids because the index-entries will be in same order */
            ArrayList<Long> sortedIds = new ArrayList<Long>(queryIds);
            Collections.sort(sortedIds);

            ArrayList<ShardIndex.IndexEntry> indexEntries = new ArrayList<ShardIndex.IndexEntry>(sortedIds.size());
            for(Long a : sortedIds) {
                indexEntries.add(index.lookup(a));
            }

            ShardIndex.IndexEntry entry = null;
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = indexEntries.get(qIdx);
                long vertexId = sortedIds.get(qIdx);

                long curPtr = findIdxAndPos(vertexId, entry);

                if (curPtr != (-1L)) {
                    long nextPtr = pointerIdxBuffer.get();
                    int n = (int) (VertexIdTranslate.getAux(nextPtr) - VertexIdTranslate.getAux(curPtr));

                    ArrayList<Long> res = new ArrayList<Long>(n);
                    ArrayList<Long> resPointers = new ArrayList<Long>(n);

                    long adjOffset = VertexIdTranslate.getAux(curPtr);
                    adjBuffer.position((int)adjOffset);
                    for(int i=0; i<n; i++) {
                        res.add(VertexIdTranslate.getVertexId(adjBuffer.get()));
                        resPointers.add(PointerUtil.encodePointer(shardNum, (int) adjOffset + i));
                    }
                    callback.receiveOutNeighbors(vertexId, res, resPointers);
                } else {
                    callback.receiveOutNeighbors(vertexId, new ArrayList<Long>(0), new ArrayList<Long>(0));
                }
            }
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }


    /**
     * Returns the vertex idx for given vertex in the pointer file, or -1 if not found.
     */
    private long findIdxAndPos(long vertexId, ShardIndex.IndexEntry sparseIndexEntry) {

        assert(sparseIndexEntry.vertex <= vertexId);

        if (pointerIdxBuffer.capacity() == 0) return -1;

        int vertexSeq = sparseIndexEntry.vertexSeq;
        long curvid = sparseIndexEntry.vertex;
        pointerIdxBuffer.position(vertexSeq);
        long ptr = pointerIdxBuffer.get();

        while(curvid < vertexId) {
            try {
                curvid = VertexIdTranslate.getVertexId(ptr);
                if (curvid == vertexId) {
                    return ptr;
                }
                ptr = pointerIdxBuffer.get();
            } catch (BufferUnderflowException bufe) {
                return -1;
            }
        }
        return -1L;
    }

    public synchronized void queryIn(Long queryId, QueryCallback callback) {

        if (queryId < interval.getFirstVertex() || queryId > interval.getLastVertex()) {
            throw new IllegalArgumentException("Vertex " + queryId + " not part of interval:" + interval);
        }

        if (inEdgeStartBuffer.capacity() < (1 + queryId - interval.getFirstVertex())) {
            return;
        }

        try {
            /* Step 1: collect adj file offsets for the in-edges */
            final Timer.Context _timer1 = inEdgePhase1Timer.time();
            ArrayList<Integer> offsets = new ArrayList<Integer>();
            int END = (1<<30) - 1;
            int off = inEdgeStartBuffer.get((int) (queryId - interval.getFirstVertex()));

            while(off != END) {
                offsets.add(off);
                adjBuffer.position(off);
                long edge = adjBuffer.get();
                if (VertexIdTranslate.getVertexId(edge) != queryId) {
                    throw new RuntimeException("Mismatch in edge linkage: " + VertexIdTranslate.getVertexId(edge) + " !=" + queryId);
                }
                off = (int) VertexIdTranslate.getAux(edge);
                if (off > END) {
                    throw new RuntimeException("Encoding error: " + edge + " --> " + VertexIdTranslate.getVertexId(edge)
                        + " off : " + VertexIdTranslate.getAux(edge));
                }
            }
            _timer1.stop();

            /* Step 2: collect the vertex ids that contain the offsets by passing over the pointer data */
            /* Find beginning */


            ArrayList<Long> inNeighbors = new ArrayList<Long>(offsets.size());
            ArrayList<Long> inNeighborsPtrs = new ArrayList<Long>(offsets.size());

            Iterator<Integer> offsetIterator = offsets.iterator();
            if (!offsets.isEmpty()) {
                final Timer.Context  _timer2 = inEdgePhase2Timer.time();

                int firstOff = offsets.get(0);
                final Timer.Context  _timer3 = inEdgeIndexLookupTimer.time();
                ShardIndex.IndexEntry startIndex = index.lookupByOffset(firstOff * 8);
                _timer3.stop();

                pointerIdxBuffer.position(startIndex.vertexSeq);

                long last = pointerIdxBuffer.get();
                while (offsetIterator.hasNext()) {
                    off = offsetIterator.next();
                    long ptr = last;

                    while(VertexIdTranslate.getAux(ptr) <= off) {
                        last = ptr;
                        ptr = pointerIdxBuffer.get();
                    }
                    inNeighbors.add(VertexIdTranslate.getVertexId(last));
                    inNeighborsPtrs.add(PointerUtil.encodePointer(shardNum, off));
                }
                _timer2.stop();

            }
            callback.receiveInNeighbors(queryId, inNeighbors, inNeighborsPtrs);


        } catch (Exception err) {
            throw new RuntimeException(err);
        }
        callback.receiveInNeighbors(queryId, new ArrayList<Long>(0), new ArrayList<Long>(0));
    }


    public static String namify(String baseFilename, Long vertexId) throws IOException {
        File f = new File(baseFilename + "_names.dat");
        if (!f.exists()) {
            //	System.out.println("didn't find name file: " + f.getPath());
            return vertexId+"";
        }
        long i = vertexId * 16;
        java.io.RandomAccessFile raf = new java.io.RandomAccessFile(f.getAbsolutePath(), "r");
        raf.seek(i);
        byte[] tmp = new byte[16];
        raf.read(tmp);
        raf.close();
        return new String(tmp) + "(" + vertexId + ")";
    }

    public EdgeIterator edgeIterator() {
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

            @Override
            public boolean hasNext() {
                return idx < numEdges - 1;
            }

            @Override
            public void next() {
                idx++;
                if (idx == nextOff) {
                    curSrc = VertexIdTranslate.getVertexId(nextPtr);
                    nextPtr = iterPointerBuffer.get();
                    nextOff = VertexIdTranslate.getAux(nextPtr);
                }
            }

            @Override
            public long getSrc() {
                return curSrc;
            }

            @Override
            public long getDst() {
                return VertexIdTranslate.getVertexId(iterBuffer.get());
            }
        };
    }
}
