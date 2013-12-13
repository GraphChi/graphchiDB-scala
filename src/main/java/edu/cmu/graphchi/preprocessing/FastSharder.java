package edu.cmu.graphchi.preprocessing;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.datablocks.BytesToValueConverter;
import static edu.cmu.graphchi.util.Sorting.*;

import java.io.*;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Logger;

/**
 *
 * @author Aapo Kyrola
 */
public class FastSharder <VertexValueType, EdgeValueType> {




    /**
     * Temporarily static method. Note, after this the edgeValues are sorted.
     * @param baseFilename
     * @param shardNum
     * @param numShards
     * @param sizeOf
     * @param shoveled
     * @param shoveled2   -- needs to have edge type encoded
     * @param edgeValues
     * @param minTarget
     * @param maxTarget
     * @throws IOException
     */
    public static  void writeAdjacencyShard(String baseFilename, int shardNum, int numShards, int
            sizeOf, long[] shoveled, long[] shoveled2, byte[] edgeValues, long minTarget, long maxTarget,
                                                boolean alreadySorted) throws IOException {
    /* Sort the edges */

        if (shoveled.length != shoveled2.length) {
            throw new IllegalStateException("src and dst array lengths differ:" + shoveled.length + "/" + shoveled2.length);
        }
        if (shoveled.length != edgeValues.length / sizeOf) {
            throw new IllegalStateException("Mismatch in array size: expected " + shoveled.length + " / got: " +
                    (edgeValues.length / sizeOf) + "; sizeof=" + sizeOf);
        }

        if (!alreadySorted) {
            sortWithValues(shoveled, shoveled2, edgeValues, sizeOf);  // The source id is  higher order, so sorting the longs will produce right result
        }

        /* Extract types before sorting for linking  */
        byte[] edgeTypeArray = new byte[shoveled.length];
        for(int i=0; i<shoveled.length; i++) {
            edgeTypeArray[i] = VertexIdTranslate.getType(shoveled2[i]);
            shoveled2[i] = VertexIdTranslate.getVertexId(shoveled2[i]);
        }

        int[] indices =  radixSortWithIndex(shoveled2);

        File startIdxFile = new File(ChiFilenames.getFilenameShardsAdjStartIndices(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards)));
        DataOutputStream startOutFile = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(startIdxFile)));

        long prev = -1;
        int c = shoveled2.length - 1;
        for(int i=0; i<c; i++) {
            long curr = shoveled2[i];
            long next = shoveled2[i + 1];
            if (curr == next)  {
                shoveled2[i] = VertexIdTranslate.encodeVertexPacket(edgeTypeArray[indices[i]], curr, indices[i + 1]);
            } else {
                shoveled2[i] = VertexIdTranslate.encodeVertexPacket(edgeTypeArray[indices[i]], curr, (1<<26) - 1);
            }

            if (VertexIdTranslate.getVertexId(shoveled2[i]) != curr) throw new IllegalStateException("Encoding error:" +
                    shoveled2[i] + ", curr=" + curr +", dec=" + VertexIdTranslate.getVertexId(shoveled2[i]));
            if (curr != prev) {
                // First
                startOutFile.writeInt((int) (curr - minTarget));
                startOutFile.writeInt(indices[i]);
            }
            prev = curr;
        }
        shoveled2[c] = VertexIdTranslate.encodeVertexPacket(edgeTypeArray[indices[c]], shoveled2[c], (1 << 26) - 1);
        startOutFile.close();

        // Sort back
        long[] tmpshoveled2 = new long[shoveled2.length];
        for(int j=0; j<shoveled2.length; j++) {
            tmpshoveled2[indices[j]] = shoveled2[j];
        }
        shoveled2 = tmpshoveled2;


        /*
         Now write the final shard in a compact form. Note that there is separate shard
         for adjacency and the edge-data. The edge-data is split and stored into 4-megabyte compressed blocks.
         */

        /**
         * Step 1: ADJACENCY SHARD
         */
        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));
        DataOutputStream adjOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(adjFile)));
        File ptrFile = new File(ChiFilenames.getFilenameShardsAdjPointers(adjFile.getAbsolutePath()));
        DataOutputStream ptrOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(ptrFile)));
        File indexFile = new File(adjFile.getAbsolutePath() + ".index");
        DataOutputStream indexOut = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)));
        long curvid = 0;
        int istart = 0;
        int edgeCounter = 0;
        int lastIndexFlush = 0;
        int edgesPerIndexEntry = 4096; // Tuned for fast shard queries

        int vertexSeq = 0;

        long lastSparseSetEntry = 0; // Optimization

        for(int i=0; i <= shoveled.length; i++) {
            long from = (i < shoveled.length ? shoveled[i] : -1);

            if (from != curvid) {
                if (from > 0 && from < curvid) throw new IllegalStateException("List of edges not in correct order!");
                /* Write index */
                if (edgeCounter - lastIndexFlush >= edgesPerIndexEntry) {
                    indexOut.writeLong(curvid);
                    indexOut.writeInt(adjOut.size());
                    indexOut.writeInt(edgeCounter);
                    indexOut.writeInt(vertexSeq);
                    lastIndexFlush = edgeCounter;
                }

                ptrOut.writeLong(VertexIdTranslate.encodeVertexPacket((byte)0, curvid, edgeCounter));
                vertexSeq++;
                for(int j=istart; j<i; j++) {
                    if (shoveled2[j] < minTarget) {
                        throw new IllegalStateException("Encoding error: " + shoveled2[j] + ", " +
                                VertexIdTranslate.getVertexId(shoveled2[j]) + " min target = " + minTarget);
                    }

                    adjOut.writeLong(shoveled2[j]);
                    edgeCounter++;
                }

                istart = i;
                curvid = from;
            }
        }

        ptrOut.writeLong(VertexIdTranslate.encodeVertexPacket((byte)0, shoveled[shoveled.length - 1], edgeCounter));


        adjOut.close();
        indexOut.close();
        ptrOut.close();
    }

    private static Random random = new Random();


    public static void createEmptyGraph(String baseFilename, int numShards, long maxVertexId) throws IOException {
        /* Delete files */
        File baseFile = new File(baseFilename);
        File parentDir = baseFile.getParentFile();
        for(File f : parentDir.listFiles()) {
            if (f.getName().startsWith(baseFile.getName())) {
                f.delete();
            }
        }

        /* Create empty shard files */
        for(int shardNum=0; shardNum<numShards; shardNum++) {
            createEmptyShard(baseFilename, numShards, shardNum);
        }

        /* Degree file */
        File degreeFile = new File(ChiFilenames.getFilenameOfDegreeData(baseFilename, false));
        degreeFile.createNewFile();

        /* Intervals */
        VertexIdTranslate idTranslate = new VertexIdTranslate(maxVertexId / numShards, numShards);
        FileWriter wr = new FileWriter(ChiFilenames.getFilenameIntervals(baseFilename, numShards));
        for(long j=1; j<=numShards; j++) {
            long a =(j * idTranslate.getVertexIntervalLength() -1);
            if (a < 0) {
                throw new RuntimeException("Overflow!" + a);
            }
            wr.write(a + "\n");
            if (a > maxVertexId) {
                maxVertexId = a;
            }
        }
        wr.close();

        wr = new FileWriter(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards));
        wr.write(idTranslate.stringRepresentation());
        wr.close();
    }

    public static void createEmptyShard(String baseFilename, int numShards, int shardNum) throws IOException {
        File adjFile = new File(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards));
        if (adjFile.exists()) adjFile.delete();
        adjFile.createNewFile();
        File ptrFile = new File(ChiFilenames.getFilenameShardsAdjPointers(adjFile.getAbsolutePath()));
        if (ptrFile.exists()) ptrFile.delete();
        ptrFile.createNewFile();
        File indexFile = new File(adjFile.getAbsolutePath() + ".index");
        if (indexFile.exists()) indexFile.delete();
        indexFile.createNewFile();
        File startIdxFile = new File(ChiFilenames.getFilenameShardsAdjStartIndices(ChiFilenames.getFilenameShardsAdj(baseFilename, shardNum, numShards)));
        if (startIdxFile.exists()) startIdxFile.delete();
        startIdxFile.createNewFile();
    }

}
