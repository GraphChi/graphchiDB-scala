package edu.cmu.graphchi.shards;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import edu.cmu.graphchi.VertexInterval;
import edu.cmu.graphchi.preprocessing.FastSharder;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Aapo Kyrola
 */
public class TestQueryShard {

    @Test
    public void testEdgeIteratorSmall() throws Exception  {
        String baseFilename = "/tmp/testshard";

        long[] srcs = new long[]{100, 99, 98, 97, 10, 0};
        long[] dsts = new long[]{1, 2, 3, 4, 5, 6};
        for(int j=0; j<dsts.length; j++) {
            dsts[j] = VertexIdTranslate.encodeVertexPacket((byte)0, dsts[j], 0);
        }

        FastSharder.writeAdjacencyShard(baseFilename, 0, 1, 1, srcs, dsts, new byte[srcs.length], 0, 101, false, null);

        Config cnf = ConfigFactory.parseFile(new File("conf/graphchidb.conf"));

        QueryShard shards = new QueryShard(baseFilename, 0, 1, new VertexInterval(0, 101, 0), cnf);

        EdgeIterator iter = shards.edgeIterator();

        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(0, iter.getSrc());
        assertEquals(6, iter.getDst());
        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(10, iter.getSrc());
        assertEquals(5, iter.getDst());
        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(97, iter.getSrc());
        assertEquals(4, iter.getDst());
        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(98, iter.getSrc());
        assertEquals(3, iter.getDst());
        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(99, iter.getSrc());
        assertEquals(2, iter.getDst());
        assertTrue(iter.hasNext());
        iter.next();
        assertEquals(100, iter.getSrc());
        assertEquals(1, iter.getDst());
        assertFalse(iter.hasNext());
    }

    @Test
    public void testEdgeIterator() throws Exception {
        String baseFilename = "/tmp/testshard";

        long[] srcs = new long[100000];
        long[] dsts = new long[100000];

        for(int i=0; i<srcs.length; i++) {
            srcs[i] = 2 * (i / 10);
            dsts[i] = i;
        }

        for(int j=0; j<dsts.length; j++) {
            dsts[j] = VertexIdTranslate.encodeVertexPacket((byte)0, dsts[j], 0);
        }

        FastSharder.writeAdjacencyShard(baseFilename, 0, 1, 1, srcs, dsts, new byte[srcs.length], 0, 100001, false, null);

        Config cnf = ConfigFactory.parseFile(new File("conf/graphchidb.conf"));

        QueryShard shards = new QueryShard(baseFilename, 0, 1, new VertexInterval(0, 100001, 0), cnf);

        /* From the beginning */
        EdgeIterator iter = shards.edgeIterator();
        for(int i=0; i<srcs.length; i++) {
            assertTrue(iter.hasNext());
            iter.next();
            assertEquals(srcs[i], iter.getSrc());
            assertEquals(VertexIdTranslate.getVertexId(dsts[i]), iter.getDst());
            assertEquals(i, iter.getIdx());
        }

        /* From the middle */
        iter = shards.edgeIterator(srcs[3330]);
        for(int i=3330; i<srcs.length; i++) {
            assertTrue(iter.hasNext());
            iter.next();
            assertEquals(srcs[i], iter.getSrc());
            assertEquals(VertexIdTranslate.getVertexId(dsts[i]), iter.getDst());
            assertEquals(i, iter.getIdx());
        }

         /* From the middle for a src which is not found */
        iter = shards.edgeIterator(srcs[3330] - 1);
        for(int i=3330; i<srcs.length; i++) {
            assertTrue(iter.hasNext());
            iter.next();
            assertEquals(srcs[i], iter.getSrc());
            assertEquals(VertexIdTranslate.getVertexId(dsts[i]), iter.getDst());
            assertEquals(i, iter.getIdx());
        }
    }
}
