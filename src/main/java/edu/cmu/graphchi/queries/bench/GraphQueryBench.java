package edu.cmu.graphchi.queries.bench;

import edu.cmu.graphchi.ChiFilenames;
import edu.cmu.graphchi.GraphChiEnvironment;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import edu.cmu.graphchi.queries.VertexQuery;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Aapo Kyrola
 */
public class GraphQueryBench {

    private VertexQuery queryEngine;
    private String comment;
    private String filename;
    private int numShards;

    private VertexIdTranslate translator;

    public GraphQueryBench(String filename, int numShards, String comment) throws IOException {
        this.numShards = numShards;
        this.filename = filename;
        this.comment = comment;
        this.translator = VertexIdTranslate.fromFile(new File(ChiFilenames.getVertexTranslateDefFile(filename, numShards)));

        queryEngine = new VertexQuery(filename, numShards);
    }

    public void bench1() throws IOException {
        Random random = new Random(260379);

        /* Create random list */
        int N = 500;
        HashSet<Long> ids = new HashSet<Long>(N);
        int numVertices = (int)ChiFilenames.numVertices(filename, numShards);

        // add some lower ids
        while (ids.size() < 100) {
            ids.add((long)random.nextInt(1000));
        }

        while(ids.size() < N) {
            ids.add((long)random.nextInt(numVertices));
        }
        String resultFilename = filename + "_querybench_" + N + "_" + comment.replace(" ", "_") +
                System.currentTimeMillis() + ".csv";

        BufferedWriter resultWriter = new BufferedWriter(new FileWriter(resultFilename));

        long st = System.currentTimeMillis();

        long checksum = 0;
        long totalIn = 0;
        long totalOut = 0;

        int j = 0;
        for(int iter=0; iter<3; ++iter) {
            Iterator<Long> it = ids.iterator();
            long iterSt = System.currentTimeMillis();
            System.out.println("Iteration " + iter + " starting");
            while(it.hasNext()) {
                j++;
                if (j % 50 == 0) {
                    System.out.println(j);
                    resultWriter.flush();
                }
                long vertexId = it.next();
                long internalId = translator.forward(vertexId);
                long t = System.currentTimeMillis();

                int inCount = queryEngine.queryInNeighbors(internalId).size();
                long inMs = System.currentTimeMillis() - t;

                t = System.currentTimeMillis();
                int outCount = queryEngine.queryOutNeighbors(internalId).size();
                long outMs = System.currentTimeMillis() - t;

                checksum += inCount + 3 * outCount;

                totalIn += inMs;
                totalOut += outMs;
                resultWriter.write(iter + "," + vertexId + "," +inCount + "," + outCount + "," + inMs + "," + outMs + "\n");

            }
            System.out.println("Iteration finished in " + (System.currentTimeMillis() - iterSt) * 0.001 + "  secs");

        }

        long total = System.currentTimeMillis() - st;
        resultWriter.close();

        String summaryFilename = resultFilename.replace(".csv", "_summary.txt");
        BufferedWriter summaryWriter = new BufferedWriter(new FileWriter(summaryFilename));
        summaryWriter.write("total," + total + "\n");
        summaryWriter.write("checksum," + checksum + "\n");
        summaryWriter.write("totalout," + totalOut+ "\n");
        summaryWriter.write("totalin," + totalIn + "\n");

        summaryWriter.close();

        System.out.println("Test [" + comment + "] finished in " + total / 1000 + "secs.");
        System.out.println("Checksum: "+ checksum);

        GraphChiEnvironment.reportMetrics();
    }

    public static void main(String[] args) throws  IOException {
        String filename = args[0];
        int numShards = Integer.parseInt(args[1]);
        String comment;

        if (args.length > 2) {
            comment = args[2];
        } else {
            BufferedReader rd = new BufferedReader(new InputStreamReader(System.in));

            System.out.print("Bench comment: ");
            comment = rd.readLine();
        }
        GraphQueryBench qbench = new GraphQueryBench(filename, numShards, comment);
        qbench.bench1();
    }

}
