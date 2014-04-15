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
package edu.cmu.graphchi.preprocessing;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 *
 */
public class TestVertexIdTranslate {

    @Test
    public void testTranslation() {
        for(int verticesInShard=10; verticesInShard < 1000000; verticesInShard += 100000) {
            for(int numShards=(10000000 / verticesInShard + 1); numShards < 100; numShards += 13) {
                VertexIdTranslate trans = new VertexIdTranslate(verticesInShard, numShards);
                for(int j=0; j < 10000000; j+=(1+j)) {
                     long vprime = trans.forward(j);
                     long back = trans.backward(vprime);
                     assertEquals(j, back);
                }


                for(int e=0; e<100; e++) {
                    assertEquals(e, trans.backward(trans.forward(e)));
                }

            }
        }
    }

    @Test
    public void testConstructFromString() {
        VertexIdTranslate tr = new VertexIdTranslate(99999, 44);
        String str = tr.stringRepresentation();

        VertexIdTranslate trReconstr = VertexIdTranslate.fromString(str);

        assertEquals(tr.getNumShards(), trReconstr.getNumShards());
        assertEquals(tr.getVertexIntervalLength(), trReconstr.getVertexIntervalLength());

        for(int j=0; j < 10000000; j+=(1+j)) {
            long v1 = tr.forward(j);
            long v2 = trReconstr.forward(j);
            assertEquals(v1, v2);
        }
    }

    @Test
    public void testEncoding() {
        for(long i = 0; i < 5000000000L; i += 1000000L) {
            for(long link=0; link<26843545L; link+=100005L) {
                long vPacket = VertexIdTranslate.encodeVertexPacket((byte) (i % 16), i, link);
                assertEquals(i, VertexIdTranslate.getVertexId(vPacket));
                assertEquals(link, VertexIdTranslate.getAux(vPacket));
                assertEquals((byte) (i % 16), VertexIdTranslate.getType(vPacket));
            }
        }

        long x = VertexIdTranslate.encodeVertexPacket((byte) 0xc, 1214077, 1<<26 -1);
        assertEquals(1214077, VertexIdTranslate.getVertexId(x));
        assertEquals(1<<26 - 1, VertexIdTranslate.getAux(x));
        assertEquals(0xc, VertexIdTranslate.getType(x));
    }

    @Test
    public void testDelete() {
        long deleted = VertexIdTranslate.encodeAsDeleted(89438, 8191);
        assertEquals(8191, VertexIdTranslate.getAux(deleted));
        assertEquals(89438, VertexIdTranslate.getVertexId(deleted));
        assertEquals(true, VertexIdTranslate.isEdgeDeleted(deleted));

    }
}
