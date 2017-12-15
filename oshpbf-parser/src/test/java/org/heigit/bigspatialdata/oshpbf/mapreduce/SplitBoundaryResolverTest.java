package org.heigit.bigspatialdata.oshpbf.mapreduce;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import org.heigit.bigspatialdata.oshpbf.OsmPrimitiveBlockIterator;
import static org.junit.Assert.*;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class SplitBoundaryResolverTest {

  @Test
  public void testDummy() throws IOException {
    return;
  }

  //@Test
  public void testResolve() throws IOException {

    try {
      File testData = new File(this.getClass().getResource("maldives.osh.pbf").toURI());

      assertTrue(testData.exists());

      int numberOfSplits = 10;
      long splitSize = testData.length() / numberOfSplits;

      List<Boundary> splitBoundaries = new ArrayList<>();

      long start = 0;
      long end = 0;
      while (start < testData.length()) {
        end = start + splitSize;
        if (end >= testData.length()) {
          end = testData.length() - 1;
        }

        splitBoundaries.add(new Boundary(start, end));
        start = end + 1;
      }

      int blockCount = 0;

      SplitBoundaryResolver resolver = new SplitBoundaryResolver();

      for (Boundary splitBoundary : splitBoundaries) {
        try (final RandomAccessFile raf = new RandomAccessFile(testData, "r")) {

          RandomAccessInputStream splitInputStream = new RandomAccessInputStream() {

            @Override
            public int read() throws IOException {
              return raf.read();
            }

            @Override
            public void seek(long pos) throws IOException {
              raf.seek(pos);
            }

            @Override
            public void readFully(long pos, byte[] buf, int off, int len) throws IOException {
              raf.seek(pos);
              raf.readFully(buf, off, len);
            }

            @Override
            public void readFully(long pos, byte[] buf) throws IOException {
              raf.seek(pos);
              raf.readFully(buf);

            }

            @Override
            public int read(long pos, byte[] buf, int off, int len) throws IOException {
              raf.seek(pos);
              return raf.read(buf, off, len);
            }

            @Override
            public long position() throws IOException {
              return raf.getFilePointer();
            }

            @Override
            public long length() throws IOException {
              return raf.length();
            }
          };

          Boundary boundary
                  = resolver.resolve(splitInputStream, splitBoundary.getStart(), splitBoundary.getEnd());

          try ( //
                  final InputStream is = new BoundaryStream(splitInputStream, boundary); //
                  final OsmPrimitiveBlockIterator pbfBlock = new OsmPrimitiveBlockIterator(is)) {
            while (pbfBlock.hasNext()) {
              pbfBlock.next();
              blockCount++;
            }
          }
        }
      }

      assertEquals(28, blockCount);
    } catch (URISyntaxException ex) {
      LoggerFactory.getLogger(SplitBoundaryResolverTest.class).error(ex.getLocalizedMessage());
    }
  }

}