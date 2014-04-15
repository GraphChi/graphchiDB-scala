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
import java.io.InputStream;

/**
 * @author Aapo Kyrola
 */
public class BitInputStream {

    private int bitOffset;
    private byte currentByte;
    private InputStream is;
    private long maxBits;
    private long bitsRead;

    public BitInputStream(InputStream is, long maxBits) throws IOException {
        this.is = is;
        this.maxBits = maxBits;

        if (maxBits > 0) currentByte = (byte) is.read();
        bitOffset = 0;
    }

    public boolean readBit() throws IOException {
        bitOffset ++;
        bitsRead ++;

        byte mask = (byte) (1 << (8-bitOffset));
        boolean bit = (currentByte & mask) != 0;

        if (bitOffset == 8 ) {
            bitOffset = 0;
            currentByte = (byte) is.read();
        }

        return bit;
    }


    public void seek(int bitIdx) {
        bitOffset = bitIdx;

    }

    public long getMaxBits() {
        return maxBits;
    }

    public void setMaxBits(long maxBits) {
        this.maxBits = maxBits;
    }

    public long getBitsRead() {
        return bitsRead;
    }

    public void setBitsRead(long bitsRead) {
        this.bitsRead = bitsRead;
    }

    public boolean eof() {
        return bitsRead >= maxBits;
    }
}
