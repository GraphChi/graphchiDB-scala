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
package edu.cmu.graphchi.bits;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * @author Aapo Kyrola
 */
public class BitStreamTests {
    @Test
    public void testInteroperability() throws Exception {
        int N = 100003;
        boolean[] data = new boolean[N];
        Random r = new Random();
        for(int i=0; i<N; i++) data[i] = r.nextInt() % 2 == 1;

        ByteArrayOutputStream bos = new ByteArrayOutputStream(N/8+1);
        BitOutputStream bitOs  = new BitOutputStream(bos);
        bitOs.writeBits(data);
        bitOs.close();

        assertEquals(N, bitOs.getBitsWritten());


        BitInputStream bitIn = new BitInputStream(new ByteArrayInputStream(bos.toByteArray()), bitOs.getBitsWritten());


        long i = 0;
        while(!bitIn.eof()) {
            assertEquals(data[(int) i++], bitIn.readBit());
        }

        assertEquals(i, bitOs.getBitsWritten());
    }
}
