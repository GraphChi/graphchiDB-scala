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
