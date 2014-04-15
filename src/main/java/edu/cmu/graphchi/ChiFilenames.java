package edu.cmu.graphchi;

import edu.cmu.graphchi.VertexInterval;

import java.io.*;
import java.util.ArrayList;

/**
 * Copyright [2014] [Aapo Kyrola / Carnegie Mellon University]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
public class ChiFilenames {


    public static String getFilenameOfDegreeData(String baseFilename, boolean sparse) {
        return baseFilename + "linked_degsjLong.bin" + (sparse ? ".sparse" : "");
    }

    public static String getFilenameShardsAdj(String baseFilename, int p, int nShards) {
        return baseFilename + ".linked_edata_java." + p + "_" + nShards + ".adjLong";
    }

    public static String getFilenameShardsAdjPointers(String adjFileName) {
        return adjFileName + ".ptr";
    }

    public static String getFilenameShardsAdjStartIndices(String adjFileName) {
        return adjFileName + ".stidx";
    }

    public static String getFilenameIntervals(String baseFilename, int nShards) {
        return baseFilename + "." + nShards + ".linked_intervalsjavaLong";
    }

    public static String getVertexTranslateDefFile(String baseFilename, int nshards) {
        return baseFilename + "." + nshards + ".linked_vtranslate";
    }


    // http://stackoverflow.com/questions/35842/how-can-a-java-program-get-its-own-process-id
    public static int getPid() {
        try {
            java.lang.management.RuntimeMXBean runtime = java.lang.management.ManagementFactory.getRuntimeMXBean();
            java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
            jvm.setAccessible(true);
            sun.management.VMManagement mgmt = (sun.management.VMManagement) jvm.get(runtime);
            java.lang.reflect.Method pid_method = mgmt.getClass().getDeclaredMethod("getProcessId");
            pid_method.setAccessible(true);
            return  (Integer) pid_method.invoke(mgmt);
        } catch (Exception err) {
            throw new RuntimeException(err);
        }
    }

    public static ArrayList<VertexInterval> loadIntervals(String baseFilename, int nShards)  throws FileNotFoundException, IOException {
        String intervalFilename = ChiFilenames.getFilenameIntervals(baseFilename, nShards);

        BufferedReader rd = new BufferedReader(new FileReader(new File(intervalFilename)));
        String line;
        long lastId = 0;
        ArrayList<VertexInterval> intervals = new ArrayList<VertexInterval>(nShards);
        while((line = rd.readLine()) != null) {
            long vid = Long.parseLong(line);
            intervals.add(new VertexInterval(lastId, vid, intervals.size()));
            lastId = vid + 1;
        }
        return intervals;
    }


    public static long numVertices(String baseFilename, int numShards) throws IOException {
        ArrayList<VertexInterval> intervals = loadIntervals(baseFilename, numShards);
        return intervals.get(intervals.size() - 1).getLastVertex() + 1;
    }
}

