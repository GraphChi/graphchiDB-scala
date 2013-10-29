package edu.cmu.graphchi.queries.demo;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.ChiLogger;
import edu.cmu.graphchi.datablocks.FloatConverter;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.VertexQuery;
import edu.cmu.graphchi.util.IdCount;
import edu.cmu.graphchi.util.MultinomialSampler;
import edu.cmu.graphchi.vertexdata.VertexAggregator;
import edu.cmu.graphchi.vertexdata.VertexIdValue;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;

/**
 * @author Aapo Kyrola
 */
public class ShortestPathQuery {


    private VertexQuery queryEngine;
    private static final Logger logger = ChiLogger.getLogger("sssp");
    private VertexIdTranslate translator;
    private String baseFilename;
    private int numShards;
    private BufferedWriter logWriter = new BufferedWriter(new FileWriter("fof.log"));

    /**
     * Construct friends-of-friends (of followees of followees in Twitter parlance)
     * engine.
     * @param baseFilename graph name
     * @param numShards number of shards
     * @param weightByPagerank whether to weight sampling by pagerank
     * @throws java.io.IOException
     */
    public ShortestPathQuery(String baseFilename, int numShards, boolean weightByPagerank) throws IOException {
        this.queryEngine = new VertexQuery(baseFilename, numShards);
        this.baseFilename = baseFilename;
        this.numShards = numShards;
        this.translator = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(baseFilename, numShards)));


    }

    public void inNeighbors(long vertexId) {
        long st = System.currentTimeMillis();
        ArrayList<Long> neighbors = queryEngine.queryInNeighbors(translator.forward(vertexId));
        System.out.println(neighbors);
        System.out.println("Querying in-neighbors (" + neighbors.size() + ") took " +
                (System.currentTimeMillis() - st) + " ms");
    }


    public static void main(String[] args) throws Exception {
        String baseFilename = args[0];
        int numShards = Integer.parseInt(args[1]);

        ShortestPathQuery sssp = new ShortestPathQuery(baseFilename, numShards, false);

        BufferedReader cmdIn = new BufferedReader(new InputStreamReader(System.in));
        while(true) {
            System.out.print("Enter vertex id to get in-neighbors >> :: ");
            String ln = cmdIn.readLine();
            if (ln.startsWith("q")) break;

            if (ln.startsWith("t")) {
                for(int i=10; i < 1000; i++) {
                    sssp.inNeighbors(i);
                }
                break;
            }
            if (ln.startsWith("b")) {
                // Benchmark
                sssp.queryEngine.shutdown();
                sssp = new ShortestPathQuery(baseFilename, numShards, false);
                long numVertices = ChiFilenames.numVertices(baseFilename, numShards);

                Random r = new Random();
                for(int i=0; i<1000000; i++) {
                    long vId = r.nextInt((int)numVertices);

                    if (vId % 10 <= 4) {    // 50% of time look for lower ids which have higher degree
                        vId = r.nextInt((int)numVertices % 100000);
                    }

                    sssp.inNeighbors(vId);

                    if (i % 1000 == 0) {
                        logger.info("Benchmark round " + i);
                    }
                    sssp.logWriter.flush();
                }
            }

            long queryId = Long.parseLong(ln);
            sssp.inNeighbors(queryId);
        }
        sssp.queryEngine.shutdown();
    }

}
