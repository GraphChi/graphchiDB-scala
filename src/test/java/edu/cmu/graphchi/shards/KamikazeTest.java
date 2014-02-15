package edu.cmu.graphchi.shards;

import com.kamikaze.docidset.api.DocSet;
import com.kamikaze.docidset.utils.DocSetFactory;
import com.kamikaze.pfordelta.PForDelta;
import edu.cmu.graphchi.preprocessing.VertexIdTranslate;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Iterator;

/**
 * @author Aapo Kyrola
 */
public class KamikazeTest {
    LongBuffer pointerIdxBuffer;

    public KamikazeTest(File pointerFile) throws Exception {
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

    public void compress() throws IOException {
        long st = System.currentTimeMillis();

        DocSet vertexIdSequence = DocSetFactory.getPForDeltaDocSetInstance();
        DocSet fileOffsetSequence = DocSetFactory.getPForDeltaDocSetInstance();

        pointerIdxBuffer.position(0);
        for(int i=0; i<pointerIdxBuffer.capacity(); i++)
        {
            long x = pointerIdxBuffer.get();
            vertexIdSequence.addDoc((int)VertexIdTranslate.getVertexId(x));
            fileOffsetSequence.addDoc((int)VertexIdTranslate.getAux(x));

        }
        vertexIdSequence.optimize();
        fileOffsetSequence.optimize();

        System.out.println("Compression took " + (System.currentTimeMillis() - st) + "ms");
        System.out.println("Sizes: vertex ids=" + vertexIdSequence.sizeInBytes()/1024.0 + " Kbytes, offsets=" + fileOffsetSequence.sizeInBytes()/1024.0);

        long totalBytes = vertexIdSequence.sizeInBytes() + fileOffsetSequence.sizeInBytes();
        System.out.println("Compression: " +  (totalBytes*1.0/((double)pointerIdxBuffer.capacity()*8)));

        System.out.println("Check...");

        pointerIdxBuffer.position(0);

        DocIdSetIterator vertexIdSeqIter = vertexIdSequence.iterator();
        DocIdSetIterator offsetIter = fileOffsetSequence.iterator();

        for(int i=0; i<pointerIdxBuffer.capacity(); i++)
        {
            long x = pointerIdxBuffer.get();
            int vid = vertexIdSeqIter.nextDoc();
            int off = offsetIter.nextDoc();

          //  if (VertexIdTranslate.getVertexId(x) != vid) {
            //    System.out.println(i + " mismatch vertexids: " + VertexIdTranslate.getVertexId(x) + ", " + vid);
            //}
            if (VertexIdTranslate.getAux(x) != off) {
                System.out.println(i + " mismatch offsets: " + VertexIdTranslate.getAux(x) + ", " + off);

            }
        }
    }

    public static void main(String[] args) throws Exception  {
       File f = new File("/Users/akyrola/graphs/DB/twitter/twitter_rv.net.linked_edata_java.319_256.adjLong.ptr");
       KamikazeTest kzt = new KamikazeTest(f);
       kzt.compress();
    }
}
