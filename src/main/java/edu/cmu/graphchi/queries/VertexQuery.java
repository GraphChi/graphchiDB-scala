package edu.cmu.graphchi.queries;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.engine.VertexInterval;
import edu.cmu.graphchi.shards.QueryShard;

/**
 * Disk-based queries of out-edges of a vertex.
 * <b>Note:</b> all vertex-ids in *internal* vertex id space.
 * @author Aapo Kyrola
 */
public class VertexQuery {

    private static final int NTHREADS = 4;

    private static final Logger logger = ChiLogger.getLogger("vertexquery");
    private ArrayList<Shard> shards;
    private ExecutorService executor;
    private ArrayList<VertexInterval> intervals;

    public VertexQuery(String baseFilename, int numShards) throws IOException{
        shards = new ArrayList<Shard>();
        for(int i=0; i<numShards; i++) {
            shards.add(new Shard(baseFilename, i, numShards));
        }
        executor = Executors.newFixedThreadPool(NTHREADS);
        intervals = ChiFilenames.loadIntervals(baseFilename, numShards);
    }




    /**
     * Queries all out neighbors of given vertices and returns a hashmap with (vertex-id, count),
     * where count is the number of queryOutAndCombine vertices who had the vertex-id as neighbor.
     * @param queryVertices
     * @return
     */
    public HashMap<Long, Integer> queryOutNeighborsAndCombine(final Collection<Long> queryVertices) {
        HashMap<Long, Integer> results;
        List<Future<HashMap<Long, Integer>>> queryFutures = new ArrayList<Future<HashMap<Long, Integer>>>();

        /* Check which ones are in cache */
        long st = System.currentTimeMillis();
        HashMap<Long, Integer> fromCache = new HashMap<Long, Integer>(1000000);

        logger.info("Cached queries took: " + (System.currentTimeMillis() - st));

        /* Execute queries in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashMap<Long, Integer>>() {
                @Override
                public HashMap<Long, Integer> call() throws Exception {
                    HashMap<Long, Integer> edges = _shard.queryOutAndCombine(queryVertices);
                    return edges;
                }
            }));
        }

        /* Combine
        */
        try {
            results = fromCache;

            for(int i=0; i < queryFutures.size(); i++) {
                HashMap<Long, Integer> shardResults = queryFutures.get(i).get();

                for(Map.Entry<Long, Integer> e : shardResults.entrySet()) {
                    if (results.containsKey(e.getKey())) {
                        results.put(e.getKey(), e.getValue() + results.get(e.getKey()));
                    } else {
                        results.put(e.getKey(), e.getValue());
                    }
                }
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }


        return  results;
    }

    /**
     * Queries out=neighbors for a given set of vertices.
     * @param queryVertices
     * @return
     */
    public HashMap<Long, ArrayList<Long>> queryOutNeighbors(final Collection<Long> queryVertices) {
        HashMap<Long,  ArrayList<Long>> results;
        List<Future<HashMap<Long, ArrayList<Long>> >> queryFutures
                = new ArrayList<Future<HashMap<Long, ArrayList<Long>> >>();

        /* Check which ones are in cache */
        long st = System.currentTimeMillis();
        HashMap<Long, ArrayList<Long>> fromCache = new HashMap<Long, ArrayList<Long>>(1000);


        /* Execute queries in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashMap<Long, ArrayList<Long>>>() {
                @Override
                public HashMap<Long, ArrayList<Long>> call() throws Exception {
                    HashMap<Long, ArrayList<Long>>  edges = _shard.query(queryVertices);
                    return edges;
                }
            }));
        }

        /* Combine
        */
        try {
            results = fromCache;

            for(int i=0; i < queryFutures.size(); i++) {
                HashMap<Long,  ArrayList<Long>> shardResults = queryFutures.get(i).get();

                for(Map.Entry<Long,  ArrayList<Long>> e : shardResults.entrySet()) {
                    ArrayList<Long> existing = results.get(e.getKey());
                    if (existing == null) {
                        results.put(e.getKey(), e.getValue());
                    } else {
                        existing.addAll(e.getValue());
                    }
                }
            }

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        return  results;
    }


    /**
     * Return out-neighbors of given vertex
     * @param internalId
     * @return
     * @throws IOException
     */
    public HashSet<Long> queryOutNeighbors(final long internalId) throws IOException  {
        HashSet<Long> friends;
        List<Future<HashSet<Long>>> queryFutures = new ArrayList<Future<HashSet<Long>>>();

        /* Query from shards in parallel */
        for(Shard shard : shards) {
            final Shard _shard = shard;
            queryFutures.add(executor.submit(new Callable<HashSet<Long>>() {
                @Override
                public HashSet<Long> call() throws Exception {
                    try {
                        return _shard.queryOut(internalId);
                    } catch (Exception err) {
                        err.printStackTrace();
                        throw new RuntimeException(err);
                    }
                }
            }));
        }
        try {
            friends = queryFutures.get(0).get();

            for(int i=1; i < queryFutures.size(); i++) {
                HashSet<Long> shardFriends = queryFutures.get(i).get();
                for(Long fr : shardFriends) {
                    friends.add(fr);
                }
            }
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
        return friends;
    }

    /**
     * Shutdowns the executor threads.
     */
    public void shutdown() {
        executor.shutdown();
    }

    public ArrayList<Long> queryInNeighbors(final long internalId) {
        /* Query from shards in parallel */
        for(int i=0; i<intervals.size(); i++) {
             if (intervals.get(i).contains(internalId)) {
                 return shards.get(i).queryInNeighbors(internalId);
             }
        }

        throw new IllegalArgumentException("Could not find shard for vertex!");
    }


    static class Shard extends QueryShard {

        private Shard(String fileName, int shardNum, int numShards) throws IOException {
            super(fileName, shardNum, numShards);
        }

        /**
         * Query efficiently all vertices
         * @param queryIds
         * @return
         * @throws IOException
         */
        public HashMap<Long, Integer> queryOutAndCombine(Collection<Long> queryIds) throws IOException {
            final HashMap<Long, Integer> results = new HashMap<Long, Integer>(5000);

            super.queryOut(queryIds, new QueryCallback() {
                @Override
                public void receiveOutNeighbors(long vertexId, ArrayList<Long> neighbors, ArrayList<Long> pointers) {
                    for (Long target : neighbors) {
                        Integer curCount = results.get(target);
                        if (curCount == null) {
                            results.put(target, 1);
                        } else {
                            results.put(target, 1 + curCount);
                        }
                    }
                }

                public void receiveInNeighbors(long vertexId, ArrayList<Long> neighbors, ArrayList<Long> pointers) {
                    throw new IllegalStateException();
                }

            });
            return results;
        }


        public HashMap<Long, ArrayList<Long>> query(Collection<Long> queryIds) throws IOException {
            final HashMap<Long, ArrayList<Long>> results = new HashMap<Long, ArrayList<Long>>(queryIds.size());
            super.queryOut(queryIds, new QueryCallback() {
                @Override
                public void receiveOutNeighbors(long vertexId, ArrayList<Long> neighbors, ArrayList<Long> pointers) {
                    results.put(vertexId, neighbors);
                }

                public void receiveInNeighbors(long vertexId, ArrayList<Long> neighbors, ArrayList<Long> pointers) {
                    throw new IllegalStateException();
                }
            });
            return results;
        }

        public HashSet<Long> queryOut(long vertexId) throws IOException {
            HashMap<Long, ArrayList<Long>> res = query(Collections.singletonList(vertexId));
            return new HashSet<Long>(res.get(vertexId));
        }


        public ArrayList<Long> queryInNeighbors(long internalId) {
            final ArrayList<Long> results = new ArrayList<Long>();
            super.queryIn(internalId, new QueryCallback() {
                public void receiveOutNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Long> pointers) {}
                public void receiveInNeighbors(long vertexId, ArrayList<Long> neighborIds, ArrayList<Long> pointers) {
                    results.addAll(neighborIds);
                }
            } );
            return results;
        }
    }


}
