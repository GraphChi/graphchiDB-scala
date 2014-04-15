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

/**
 * @author Aapo Kyrola
 */
public class PointerUtil {

    private static final int BUFFERIDSTART = (1<<24);

    public static long encodePointer(int shardNum, int shardPos) {
        return (((long)shardNum) << 32) | shardPos;
    }

    public static long encodeBufferPointer(int bufferId, int bufferpos) {
        return encodePointer(BUFFERIDSTART + bufferId, bufferpos);
    }

    public static int decodeShardNum(long pointer) {
        return (int) ((pointer >> 32) & 0xffffffffL);
    }

    public static int decodeShardPos(long pointer) {
        return (int) (pointer & 0xffffffffL);
    }

    public static int decodeBufferPos(long pointer) {
        return decodeShardPos(pointer);
    }

    public static int decodeBufferNum(long pointer) {
        return decodeShardNum(pointer) - BUFFERIDSTART;
    }

    public static boolean isBufferPointer(long pointer) {
        return decodeShardNum(pointer) >= BUFFERIDSTART;
    }
}
