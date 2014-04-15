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

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Aapo Kyrola
 */
public class BitOutputStream {


    private OutputStream out;

    private byte curByte = 0;
    private int curIdx;
    private long bitsWritten = 0;

    public BitOutputStream(OutputStream os) {
        this.out = os;
        curIdx = 0;
    }


    public void writeBits(boolean... bits) throws IOException {
        for(boolean b:bits) {
            writeBit(b);
        }
    }

    public void writeBit(boolean b) throws IOException {
        bitsWritten++;
        curIdx++;
        byte mask = (byte)(1 << (8-curIdx));
        if (b) curByte = (byte) (curByte | mask);
        if (curIdx == 8) {
            curIdx = 0;
            out.write(curByte);
            curByte = 0;
        }
    }




    public void close() throws IOException {
        if (curIdx > 0 ) out.write(curByte);
        out.flush();
    }

    public long getBitsWritten() {
        return bitsWritten;
    }

}
