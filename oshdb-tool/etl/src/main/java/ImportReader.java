import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHNodes;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHRelations;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHWays;
import org.heigit.bigspatialdata.oshdb.index.XYGrid;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.Extract.KeyValuePointer;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.Role;
import org.heigit.bigspatialdata.oshdb.tool.importer.extract.data.VF;
import org.heigit.bigspatialdata.oshdb.tool.importer.util.ZGrid;
import org.heigit.bigspatialdata.oshdb.util.OSHDBBoundingBox;
import com.google.common.io.MoreFiles;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;

public class ImportReader {

  @FunctionalInterface
  private static interface GridInstance<T> {
    T get(long xyId, int zoom, int[] index, byte[] data);
  }

  FastByteArrayOutputStream out = new FastByteArrayOutputStream(1024);

  private <T> void load(Path data, PreparedStatement stmt, GridInstance<T> instance) {
    try (DataInputStream in =
        new DataInputStream(MoreFiles.asByteSource(data).openBufferedStream())) {
      while (in.available() > 0) {
        long zid = in.readLong();
        int length = in.readInt();
        int size = in.readInt();
        int[] offsets = new int[size];
        for (int i = 0; i < size; i++) {
          offsets[i] = in.readInt();
        }
        byte[] bytes = new byte[length - size * 4 - 4];
        in.readFully(bytes);

        final int zoom = ZGrid.getZoom(zid);
        final XYGrid xyGrid = new XYGrid(zoom);
        final OSHDBBoundingBox bbox = ZGrid.getBoundingBox(zid);
        long baseLongitude =
            bbox.getMinLonLong() + (bbox.getMaxLonLong() - bbox.getMinLonLong()) / 2;
        long baseLatitude =
            bbox.getMinLatLong() + (bbox.getMaxLatLong() - bbox.getMinLatLong()) / 2;
        long xyId = xyGrid.getId(baseLongitude, baseLatitude);

        T grid = instance.get(xyId, zoom, offsets, bytes);

        try {
          out.reset();
          try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
            oos.writeObject(grid);
            oos.flush();
          }
          stmt.setInt(1, zoom);
          stmt.setLong(2, xyId);
          stmt.setBinaryStream(3, new FastByteArrayInputStream(out.array, 0, out.length));
          stmt.executeUpdate();

        } catch (IOException | SQLException e) {
          throw new RuntimeException(e);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  
  public static void main(String[] args) throws ClassNotFoundException {
    Path workdir = Paths.get("/home/rtroilo/data/idai");

    Path oshdb = workdir.resolve("oshdb");
    Class.forName("org.h2.Driver");
    try (
        Connection conn = DriverManager.getConnection("jdbc:h2:" + oshdb.toString() + "", "sa", "");
        Statement stmt = conn.createStatement()) {

      ImportReader reader = new ImportReader();
      
      Map<String, GridInstance<?>> types = new HashMap<>();
      types.put("node", (xyId, zoom, offsets, bytes) -> new GridOSHNodes(xyId, zoom, 0L, 0L, 0L, 0L, offsets, bytes));
      types.put("way", (xyId, zoom, offsets, bytes) -> new GridOSHWays(xyId, zoom, 0L, 0L, 0L, 0L, offsets, bytes));
      types.put("relation", (xyId, zoom, offsets, bytes) -> new GridOSHRelations(xyId, zoom, 0L, 0L, 0L, 0L, offsets, bytes));
      
      
      for (Entry<String, GridInstance<?>> e : types.entrySet()) {
        String type = e.getKey();
        Path data = workdir.resolve(type + ".data");
        stmt.executeUpdate(String.format( //
            "drop table if exists grid_%1$s; " + //
                "create table if not exists grid_%1$s (level int, id bigint, data blob, primary key(level,id))",
            type));
        try (PreparedStatement pstmt = conn.prepareStatement(
            String.format("insert into grid_%s (level,id,data) values(?,?,?)", type))) {
          reader.load(data, pstmt, e.getValue());
        }
      }

      stmt.executeUpdate(
          "drop table if exists metadata ; create table if not exists metadata (key varchar primary key, value varchar)");
      loadMeta(conn, workdir.resolve("extract_meta"));
      stmt.executeUpdate(
          "drop table if exists key ; create table if not exists key (id int primary key, txt varchar)");
      stmt.executeUpdate(
          "drop table if exists keyvalue; create table if not exists keyvalue (keyId int, valueId int, txt varchar, primary key (keyId,valueId))");
      loadTags(conn, workdir.resolve("extract_keys"), workdir.resolve("extract_keyvalues"));
      stmt.executeUpdate(
          "drop table if exists role ; create table if not exists role (id int primary key, txt varchar)");
      loadRoles(conn, workdir.resolve("extract_roles"));


    } catch (SQLException e) {
      e.printStackTrace();
    }

  }

  public static void loadMeta(Connection conn, Path meta) {
    try (
        PreparedStatement pstmt =
            conn.prepareStatement("insert into metadata (key,value) values(?,?)");
        BufferedReader br = new BufferedReader(new FileReader(meta.toFile()));) {


      String line = null;
      while ((line = br.readLine()) != null) {
        if (line.trim().isEmpty())
          continue;

        String[] split = line.split("=", 2);
        if (split.length != 2)
          throw new RuntimeException("metadata file is corrupt");

        pstmt.setString(1, split[0]);
        pstmt.setString(2, split[1]);
        pstmt.addBatch();
      }


      pstmt.setString(1, "attribution.short");
      pstmt.setString(2, "© OpenStreetMap contributors");
      pstmt.addBatch();

      pstmt.setString(1, "attribution.url");
      pstmt.setString(2, "https://ohsome.org/copyrights");
      pstmt.addBatch();

      pstmt.setString(1, "oshdb.maxzoom");
      pstmt.setString(2, "" + 14);
      pstmt.addBatch();

      pstmt.setString(1, "oshdb.flags");
      pstmt.setString(2, "expand");
      pstmt.addBatch();

      pstmt.executeBatch();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static void loadTags(Connection conn, Path keys, Path keyValues) {
    try (
        final DataInputStream keyIn =
            new DataInputStream(MoreFiles.asByteSource(keys).openBufferedStream());
        final RandomAccessFile raf = new RandomAccessFile(keyValues.toFile(), "r");
        final FileChannel valuesChannel = raf.getChannel();) {

      final int length = keyIn.readInt();
      int batch = 0;
      try {

        PreparedStatement insertKey =
            conn.prepareStatement("insert into key (id,txt) values (?,?)");
        PreparedStatement insertValue =
            conn.prepareStatement("insert into keyvalue ( keyId, valueId, txt ) values(?,?,?)");

        for (int keyId = 0; keyId < length; keyId++) {
          final KeyValuePointer kvp = KeyValuePointer.read(keyIn);
          final String key = kvp.key;

          insertKey.setInt(1, keyId);
          insertKey.setString(2, key);
          insertKey.executeUpdate();
          valuesChannel.position(kvp.valuesOffset);

          DataInputStream valueStream = new DataInputStream(Channels.newInputStream(valuesChannel));

          long chunkSize = (long) Math.ceil((double) (kvp.valuesNumber / 10.0));
          int valueId = 0;
          for (int i = 0; i < 10; i++) {
            long chunkEnd = valueId + Math.min(kvp.valuesNumber - valueId, chunkSize);
            for (; valueId < chunkEnd; valueId++) {
              final VF vf = VF.read(valueStream);
              final String value = vf.value;

              insertValue.setInt(1, keyId);
              insertValue.setInt(2, valueId);
              insertValue.setString(3, value);
              insertValue.addBatch();
              batch++;

              if (batch >= 100_000) {
                insertValue.executeBatch();
                batch = 0;
              }
            }
          }
        }
        insertValue.executeBatch();
      } catch (SQLException e) {
        throw new IOException(e);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void loadRoles(Connection conn, Path roles) {
    try (DataInputStream roleIn =
        new DataInputStream(MoreFiles.asByteSource(roles).openBufferedStream())) {
      try {

        PreparedStatement insertRole =
            conn.prepareStatement("insert into role (id,txt) values(?,?)");
        for (int roleId = 0; roleIn.available() > 0; roleId++) {
          final Role role = Role.read(roleIn);

          insertRole.setInt(1, roleId);
          insertRole.setString(2, role.role);
          insertRole.executeUpdate();
          System.out.printf("load role:%6d(%s)%n", roleId, role.role);

        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    } catch (IOException e1) {
      e1.printStackTrace();
    }
  }


}
