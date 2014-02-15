package edu.cmu.graphchi.shards;

import edu.cmu.graphchi.preprocessing.VertexIdTranslate;

import java.io.File;
import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Aapo Kyrola
 */
public class GapEncodingTest {
    LongBuffer pointerIdxBuffer;

    public GapEncodingTest(File pointerFile) throws Exception {
        System.out.println("Reading fully: " + pointerFile.getName());
        byte[] data = new byte[(int)pointerFile.length()];
        FileInputStream fis = new FileInputStream(pointerFile);
        int i = 0;
        while (i < data.length) {
            i += fis.read(data, i, data.length - i);
        }
        fis.close();

        pointerIdxBuffer = ByteBuffer.wrap(data).asLongBuffer();
        System.out.println("buffer size: "  + pointerIdxBuffer.capacity());
    }


    public void distribution() {
        HashMap<Long, Integer> distr = new HashMap<>();
        long lastvid = 0;
        for(int i=0; i<pointerIdxBuffer.capacity(); i++)
        {
            long x = pointerIdxBuffer.get();
            long vid = VertexIdTranslate.getVertexId(x);

            long gap = vid - lastvid;

            if (distr.containsKey(gap)) {
                distr.put(gap, distr.get(gap) + 1);
             } else {
                distr.put(gap, 1);
            }

            lastvid = vid;

        }

        for(Map.Entry<Long, Integer> e : distr.entrySet()) {
           System.out.println(e.getKey() + "," + e.getValue());
        }

    }

    public static void main(String[] args) throws Exception  {
        File f = new File("/Users/akyrola/graphs/DB/twitter/twitter_rv.net.linked_edata_java.319_256.adjLong.ptr");
        GapEncodingTest get = new GapEncodingTest(f);
        get.distribution();
    }
}
