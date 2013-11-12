package edu.cmu.graphchi.queries;

/**
 * @author Aapo Kyrola
 */
public class PointerUtil {

    public static long encodePointer(int shardNum, int shardPos) {
        return (((long)shardNum) << 32) | shardPos;
    }

    public static int decodeShardNum(long pointer) {
        return (int) ((pointer >> 32) & 0xffffffffL);
    }

    public static int decodeShardPos(long pointer) {
        return (int) (pointer & 0xffffffffL);
    }
}
