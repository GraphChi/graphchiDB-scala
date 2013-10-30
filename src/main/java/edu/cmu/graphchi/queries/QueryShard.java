package edu.cmu.graphchi.queries;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.shards.ShardIndex;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.nio.LongBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystems;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Shard query support
 * @author Aapo Kyrola
 */
public class QueryShard {

    private RandomAccessFile adjFileInput;
    private ShardIndex index;
    private int shardNum;
    private int numShards;
    private String fileName;
    private File adjFile;

    private LongBuffer pointerIdxBuffer;


    public QueryShard(String fileName, int shardNum, int numShards) throws IOException {
        this.shardNum = shardNum;
        this.numShards = numShards;
        this.fileName = fileName;
        adjFile = new File(ChiFilenames.getFilenameShardsAdj(fileName, shardNum, numShards));
        adjFileInput = new RandomAccessFile(adjFile.getAbsolutePath(), "r", 64 * 1024);
        index = new ShardIndex(adjFile);
        loadPointers();
    }

    void loadPointers() throws IOException {
        File pointerFile = new File(ChiFilenames.getFilenameShardsAdjPointers(adjFile.getAbsolutePath()));
        FileChannel ptrFileChannel = new java.io.RandomAccessFile(pointerFile, "r").getChannel();
        pointerIdxBuffer = ptrFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, pointerFile.length()).asLongBuffer();
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

            ShardIndex.IndexEntry entry = null, lastEntry = null;
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = indexEntries.get(qIdx);
                long vertexId = sortedIds.get(qIdx);

                long curPtr = findIdxAndPos(vertexId, entry);

                if (curPtr != (-1L)) {
                    long nextPtr = pointerIdxBuffer.get();
                    int n = (int) (VertexIdTranslate.getAux(nextPtr) - VertexIdTranslate.getAux(curPtr));

                    ArrayList<Long> res = new ArrayList<Long>(n);
                    long adjOffset = VertexIdTranslate.getAux(curPtr);
                    adjFileInput.seek(adjOffset * 8);
                    for(int i=0; i<n; i++) {
                        res.add(VertexIdTranslate.getVertexId(Long.reverseBytes(adjFileInput.readLong())));
                    }
                    callback.receiveOutNeighbors(vertexId, res);
                } else {
                    callback.receiveOutNeighbors(vertexId, new ArrayList<Long>(0));
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

        int vertexSeq = sparseIndexEntry.vertexSeq;
        long curvid = sparseIndexEntry.vertex;
        pointerIdxBuffer.position(vertexSeq);
        long ptr = pointerIdxBuffer.get();

        while(curvid < vertexId) {
            curvid = VertexIdTranslate.getVertexId(ptr);
            if (curvid == vertexId) {
                return ptr;
            }
            ptr = pointerIdxBuffer.get();
        }
        return -1L;
    }

    public synchronized void queryIn(Long queryId, QueryCallback callback) {
        ArrayList<Long> inNeighbors = new ArrayList<Long>();

        try {
            adjFileInput.seek(0);
            long curvid = 0;
            while(true) {
                int n;
                int ns = adjFileInput.readUnsignedByte();
                assert(ns >= 0);

                if (ns == 0) {
                    curvid++;
                    int nz = adjFileInput.readUnsignedByte();

                    assert(nz >= 0);
                    curvid += nz;

                    if (nz == 254) {
                        long nnz = Long.reverseBytes(adjFileInput.readLong());
                        curvid += nnz + 1;
                    }

                    continue;
                }

                if (ns == 0xff) {
                    n = adjFileInput.readInt();
                } else {
                    n = ns;
                }

                while (--n >= 0) {
                    long target = Long.reverseBytes(adjFileInput.readLong());
                    if (target == queryId)  {
                        inNeighbors.add(curvid);
                        adjFileInput.skipBytes(n * 8);
                        break;
                    }
                }
                curvid++;

            }

        } catch (java.io.EOFException eof) {
            // Kosher
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
        callback.receiveInNeighbors(queryId, inNeighbors);
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
}
