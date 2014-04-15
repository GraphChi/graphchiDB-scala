/**
 * @author  Aapo Kyrola <akyrola@cs.cmu.edu>
 * @version 1.0
 *
 * @section LICENSE
 *
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
 *
 * Publication to cite:  http://arxiv.org/abs/1403.0701
 */
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
