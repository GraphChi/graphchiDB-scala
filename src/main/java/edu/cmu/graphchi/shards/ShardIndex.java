package edu.cmu.graphchi.shards;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Encapsulates a sparse index to shard's edges.
 * Can be used for fast queries or for parallelizing access (memoryshard).
 */
public class ShardIndex {
    File indexFile;
    private long[] vertices;
    private int[] edgePointer;
    private int[] fileOffset;
    private int[] vertexSeq;

    public ShardIndex(File adjFile) throws IOException {
        this.indexFile = new File(adjFile.getAbsolutePath() + ".index");
        load();
    }

    private void load() throws IOException {
        int n = (int) (indexFile.length() / 20) + 1;
        vertices = new long[n];
        edgePointer = new int[n];
        fileOffset = new int[n];
        vertexSeq = new int[n];

        vertices[0] = 0;
        edgePointer[0] = 0;
        fileOffset[0] = 0;
        vertexSeq[0] = 0;

        DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(indexFile)));
        int i = 1;
        while (i < n) {
            vertices[i] = dis.readLong();
            fileOffset[i] = dis.readInt();
            edgePointer[i] = dis.readInt();
            vertexSeq[i] = dis.readInt();
            i++;
        }
    }

    /**
     * Returns a sparsified index, which starts with a zero-pointer (and is thus always non-empty)
     */
    public ArrayList<IndexEntry> sparserIndex(int edgeDistance) {
        ArrayList<IndexEntry> spIdx = new ArrayList<IndexEntry>();
        spIdx.add(new IndexEntry(0, 0, 0, 0));
        int lastEdgePointer = 0;
        for(int j=0; j<vertices.length; j++) {
            if (edgePointer[j] - lastEdgePointer >= edgeDistance) {
                 spIdx.add(new IndexEntry(vertices[j], edgePointer[j], fileOffset[j], vertexSeq[j]));
                 lastEdgePointer = edgePointer[j];
            }

        }
        return spIdx;
    }

    /**
     * Finds closest index entry for given vertex id, which is before the vertex.
     * @param vertexId
     * @return
     */
    public IndexEntry lookup(long vertexId) {
        int idx = Arrays.binarySearch(vertices, vertexId);
        if (idx >= 0) {
            return new IndexEntry(vertexId, edgePointer[idx], fileOffset[idx], vertexSeq[idx]);
        } else {
            idx = -(idx + 1) - 1;
            return new IndexEntry(vertices[idx], edgePointer[idx], fileOffset[idx], vertexSeq[idx]);
        }

    }



    public static class IndexEntry {

        public long vertex;
        public int edgePointer, fileOffset, vertexSeq;

        IndexEntry(long vertex, int edgePointer, int fileOffset, int vertexSeq) {
            this.vertex = vertex;
            this.edgePointer = edgePointer;
            this.fileOffset = fileOffset;
            this.vertexSeq = vertexSeq;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexEntry that = (IndexEntry) o;

            if (edgePointer != that.edgePointer) return false;
            if (fileOffset != that.fileOffset) return false;
            if (vertex != that.vertex) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = (int)vertex;
            result = 31 * result + edgePointer;
            result = 31 * result + fileOffset;
            return result;
        }

        public String toString() {
            return "vertex: " + vertex + ", offset=" + fileOffset;
        }
    }
}