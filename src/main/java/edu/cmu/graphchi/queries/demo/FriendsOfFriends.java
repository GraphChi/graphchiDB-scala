package edu.cmu.graphchi.queries.demo;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.QueryShard;
import edu.cmu.graphchi.queries.VertexQuery;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.util.MultinomialSampler;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.vertexdata.VertexIdValue;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * Demonstration of the queryOutAndCombine capabilities of GraphChi.
 * With this app, after you have computed PageRank for each vertex,
 * you can make simple friends-recommendations queries: querying for user X
 * will recommend the top friends (out-edges) of user X's friends (out-edges).
 * @author Aapo Kyrola
 */
public class FriendsOfFriends {

    private VertexQuery queryEngine;
    private static final Logger logger = ChiLogger.getLogger("fof");
    private VertexIdTranslate translator;
    private String baseFilename;
    private boolean weightByPagerank;
    private int numShards;
    private float[] ranks;  // Note: ranks are by the *internal vertex id*.
    private BufferedWriter logWriter = new BufferedWriter(new FileWriter("fof.log"));

    /**
     * Construct friends-of-friends (of followees of followees in Twitter parlance)
     * engine.
     * @param baseFilename graph name
     * @param numShards number of shards
     * @param weightByPagerank whether to weight sampling by pagerank
     * @throws IOException
     */
    public FriendsOfFriends(String baseFilename, int numShards, boolean weightByPagerank) throws IOException {
        this.queryEngine = new VertexQuery(baseFilename, numShards);
        this.baseFilename = baseFilename;
        this.weightByPagerank = weightByPagerank;
        this.numShards = numShards;
        this.translator = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)));

        if (weightByPagerank) {
            loadRanks();
        }
    }

    private void loadRanks() throws IOException {
        logger.info("Loading ranks...");
        long st = System.currentTimeMillis();
        long numVertices = ChiFilenames.numVertices(baseFilename, numShards);
        ranks = new float[(int)numVertices];
        Iterator<VertexIdValue<Float>> iter = VertexAggregator.vertexIterator(numVertices, baseFilename,
                new FloatConverter(), VertexIdTranslate.identity());
        while(iter.hasNext()) {
            VertexIdValue<Float> idVal = iter.next();
            ranks[(int)idVal.getVertexId()] = idVal.getValue();
        }
        logger.info("Loaded ranks to memory in " + (System.currentTimeMillis() - st) + " ms");
    }


    /**
     * Recommend friends
     * @param vertexId  queryOutAndCombine vertex
     * @param fanOut maximum of queryOutAndCombine vertex's friends to consider. Selected randomly.
     * @throws IOException
     */
    public String recommendFriends(long vertexId, int fanOut) throws IOException {
        long internalId = translator.forward(vertexId);

        logger.info("Querying for " + QueryShard.namify(baseFilename, vertexId) + " --> " + internalId);

        long stTime = System.currentTimeMillis();

        int total = 0;
        HashMap<Long, Integer> friendsOfFriends;
        final HashSet<Long> friends = queryEngine.queryOutNeighbors(internalId);

        long t = System.currentTimeMillis() - stTime;
        logger.info("Found " + friends.size() + " friends  in " + t + " ms.");
        int origFriendsSize = friends.size();

        Random r = new Random();
        if (friends.size() > fanOut) {

            ArrayList<Long> friendsAll = new ArrayList<Long>(friends);
            friends.clear();

            if (ranks != null) {
                // Going to sample by PageRank
                float[] weights = new float[friendsAll.size()];
                for(int i=0; i<weights.length; i++) {
                    weights[i] = ranks[friendsAll.get(i).intValue()];
                }
                int[] samples = MultinomialSampler.generateSamplesAliasMethod(r, weights, fanOut * 2);
                for(int i : samples) {
                    friends.add(friendsAll.get(i));
                    if (friends.size() == fanOut) break;
                }
            } else {
                for(int i=0; i < fanOut; i++) friends.add(friendsAll.get(Math.abs(r.nextInt()) % friendsAll.size()));
            }
        }

        if (friends.size() == 0) {
            return "";
        }


        stTime = System.currentTimeMillis();

        friendsOfFriends = queryEngine.queryOutNeighborsAndCombine(friends);
        friendsOfFriends.remove(internalId);

        long t2 = (System.currentTimeMillis() - stTime);

        for(long friend : friends) friendsOfFriends.remove(friend);
        logger.info("Found " + friendsOfFriends.size() + " friends-of-friends (that are not friends) in " +
                t2 + "ms");

        /* Take only top */
        int k = 20;
        TreeSet<IdCount> counts = new TreeSet<IdCount>();
        for(Map.Entry<Long, Integer> e : friendsOfFriends.entrySet()) {
            if (counts.size() < k) {
                counts.add(new IdCount(translator.backward(e.getKey()), e.getValue()));
            } else {
                int smallest = counts.last().count;
                if (e.getValue() > smallest) {
                    //counts.remove(counts.last());
                    counts.pollLast();
                    counts.add(new IdCount(translator.backward(e.getKey()), e.getValue()));
                }
            }
        }

        String result = "";

        for(IdCount top : counts) {
            System.out.println(QueryShard.namify(baseFilename, top.id) + " : " + top.count);
            result += QueryShard.namify(baseFilename, top.id) + " : " + top.count + "\n";
        }

        logWriter.write(origFriendsSize + "," + t + "," + t2 + "\n");
        return result;

    }


    public static void main(String[] args) throws Exception {
        String baseFilename = args[0];
        int numShards = Integer.parseInt(args[1]);

        FriendsOfFriends fof = new FriendsOfFriends(baseFilename, numShards, false);

        BufferedReader cmdIn = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("Enter vertex id to get friends-of-friends >> :: ");
            String ln = cmdIn.readLine();
            if (ln.startsWith("q")) break;

            if (ln.startsWith("t")) {
                for(int i=10; i < 1000; i++) {
                    fof.recommendFriends(i, 1000);
                }
                break;
            }
            if (ln.startsWith("b")) {
                // Benchmark
                fof.queryEngine.shutdown();
                fof = new FriendsOfFriends(baseFilename, numShards, false);
                long numVertices = ChiFilenames.numVertices(baseFilename, numShards);

                Random r = new Random();
                for(int i=0; i<1000000; i++) {
                    long vId = r.nextInt((int) numVertices);

                    if (vId % 10 <= 4) {    // 50% of time look for lower ids which have higher degree
                        vId = r.nextInt((int)numVertices % 100000);
                    }

                    fof.recommendFriends(vId, 4000);

                    if (i % 1000 == 0) {
                        logger.info("Benchmark round " + i);
                    }
                    fof.logWriter.flush();
                }
            }

            long queryId = Long.parseLong(ln);
            fof.recommendFriends(queryId, 500);
        }
        fof.queryEngine.shutdown();
    }
}
