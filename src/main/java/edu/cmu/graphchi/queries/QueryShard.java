package edu.cmu.graphchi.queries;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.shards.ShardIndex;
import ucar.unidata.io.RandomAccessFile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Shard query support
 * @author Aapo Kyrola
 */
public class QueryShard {

    private RandomAccessFile adjFile;
    private ShardIndex index;
    private int shardNum;
    private int numShards;
    private String fileName;


    public QueryShard(String fileName, int shardNum, int numShards) throws IOException {
        this.shardNum = shardNum;
        this.numShards = numShards;
        this.fileName = fileName;
        File f = new File(ChiFilenames.getFilenameShardsAdj(fileName, shardNum, numShards));
        adjFile = new RandomAccessFile(f.getAbsolutePath(), "r", 64 * 1024);
        index = new ShardIndex(f);
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
            long curvid=0, adjOffset=0;
            for(int qIdx=0; qIdx < sortedIds.size(); qIdx++) {
                entry = indexEntries.get(qIdx);
                long vertexId = sortedIds.get(qIdx);

                /* If consecutive vertices are in same indexed block, i.e their
                   index entries are the same, then we just continue.
                 */
                if (qIdx == 0 || !entry.equals(lastEntry))   {
                    curvid = entry.vertex;
                    adjOffset = entry.fileOffset;
                    adjFile.seek(adjOffset);
                }
                boolean found = false;
                while(curvid <= vertexId) {
                    int n;
                    int ns = adjFile.readUnsignedByte();
                    assert(ns >= 0);
                    adjOffset++;

                    if (ns == 0) {
                        curvid++;
                        int nz = adjFile.readUnsignedByte();

                        adjOffset++;
                        assert(nz >= 0);
                        curvid += nz;

                        if (nz == 254) {
                            long nnz = Long.reverseBytes(adjFile.readLong());
                            adjOffset += 8;
                            curvid += nnz + 1;
                        }

                        continue;
                    }

                    if (ns == 0xff) {
                        n = adjFile.readInt();
                        adjOffset += 4;
                    } else {
                        n = ns;
                    }

                    if (curvid == vertexId) {
                        ArrayList<Long> targets = new ArrayList<Long>(n);
                        while (--n >= 0) {
                            long target = Long.reverseBytes(adjFile.readLong());
                            targets.add(target);
                        }
                        callback.receiveOutNeighbors(curvid, targets);
                        found = true;
                    } else {
                        adjFile.skipBytes(n * 8);
                    }
                    curvid++;

                }
                if (!found) {
                    callback.receiveOutNeighbors(vertexId, new ArrayList<Long>(0));
                }
            }
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    public synchronized void queryIn(Long queryId, QueryCallback callback) {
        ArrayList<Long> inNeighbors = new ArrayList<Long>();

        try {
            adjFile.seek(0);
            long curvid = 0;
            while(true) {
                int n;
                int ns = adjFile.readUnsignedByte();
                assert(ns >= 0);

                if (ns == 0) {
                    curvid++;
                    int nz = adjFile.readUnsignedByte();

                    assert(nz >= 0);
                    curvid += nz;

                    if (nz == 254) {
                        long nnz = Long.reverseBytes(adjFile.readLong());
                        curvid += nnz + 1;
                    }

                    continue;
                }

                if (ns == 0xff) {
                    n = adjFile.readInt();
                } else {
                    n = ns;
                }

                ArrayList<Long> targets = new ArrayList<Long>(n);
                while (--n >= 0) {
                    long target = Long.reverseBytes(adjFile.readLong());
                    if (target == queryId)  {
                        inNeighbors.add(curvid);
                        adjFile.skipBytes(n * 8);
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
