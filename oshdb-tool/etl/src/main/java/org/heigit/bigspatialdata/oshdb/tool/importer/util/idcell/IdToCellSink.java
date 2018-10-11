package org.heigit.bigspatialdata.oshdb.tool.importer.util.idcell;

import java.io.Closeable;
import java.io.IOException;

public interface IdToCellSink extends Closeable {

	public void put(long key, long value) throws IOException;
}
