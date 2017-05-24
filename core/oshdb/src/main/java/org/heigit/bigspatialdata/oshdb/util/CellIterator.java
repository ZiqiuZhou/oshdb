package org.heigit.bigspatialdata.oshdb.util;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.TopologyException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.heigit.bigspatialdata.oshdb.grid.GridOSHEntity;
import org.heigit.bigspatialdata.oshdb.osh.OSHEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.util.tagInterpreter.TagInterpreter;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Stream;


public class CellIterator {

  public static Stream<Map<Long, Pair<OSMEntity, Geometry>>> iterateAll(GridOSHEntity cell, BoundingBox boundingBox, List<Long> timestamps, TagInterpreter tagInterpreter, Predicate<OSMEntity> osmEntityFilter, boolean includeOldStyleMultipolygons) {
    List<Map<Long, Pair<OSMEntity, Geometry>>> results = new ArrayList<>();

    for (OSHEntity<OSMEntity> oshEntity : (Iterable<OSHEntity<OSMEntity>>) cell) {
      if (!oshEntity.intersectsBbox(boundingBox)) {
        // this osh entity is fully outside the requested bounding box -> skip it
        continue;
      }
      boolean fullyInside = oshEntity.insideBbox(boundingBox);

      // optimize loop by requesting modification timestamps first, and skip geometry calculations where not needed
      SortedMap<Long, List<Long>> queryTs = new TreeMap<>();
      if (!includeOldStyleMultipolygons) {
        List<Long> modTs = oshEntity.getModificationTimestamps(osmEntityFilter);
        int j = 0;
        for (long requestedT : timestamps) {
          boolean needToRequest = false;
          while (j < modTs.size() && modTs.get(j) < requestedT) {
            needToRequest = true;
            j++;
          }
          if (needToRequest)
            queryTs.put(requestedT, new LinkedList<>());
          else if (queryTs.size() > 0)
            queryTs.get(queryTs.lastKey()).add(requestedT);
        }
      } else {
        // todo: make this work with old style multipolygons!!?!
        for (Long ts : timestamps)
          queryTs.put(ts, new LinkedList<>());
      }

      SortedMap<Long, OSMEntity> osmEntityByTimestamps = oshEntity.getByTimestamps(new ArrayList<>(queryTs.keySet()));
      Map<Long, Pair<OSMEntity, Geometry>> oshResult = new TreeMap<>();

      osmEntityLoop:
      for (Map.Entry<Long, OSMEntity> entity : osmEntityByTimestamps.entrySet()) {
        Long timestamp = entity.getKey();
        OSMEntity osmEntity = entity.getValue();

        if (!osmEntity.isVisible()) {
          // skip because this entity is deleted at this timestamp
          continue;
        }

        if (includeOldStyleMultipolygons &&
            osmEntity instanceof OSMRelation &&
            tagInterpreter.isOldStyleMultipolygon((OSMRelation) osmEntity)
            ) {
          OSMRelation rel = (OSMRelation) osmEntity;
          for (int i = 0; i < rel.getMembers().length; i++) {
            if (rel.getMembers()[i].getType() == OSHEntity.WAY && tagInterpreter.isMultipolygonOuterMember(rel.getMembers()[i])) {
              OSMEntity way = rel.getMembers()[i].getEntity().getByTimestamp(timestamp);
              if (!osmEntityFilter.test(way)) {
                // skip this old-style-multipolygon because it doesn't match our filter
                continue osmEntityLoop;
              } else {
                // we know this multipolygon only has exactly one outer way, so we can abort the loop and actually
                // "continue" with the calculations ^-^
                break;
              }
            }
          }
        } else {
          if (!osmEntityFilter.test(osmEntity)) {
            // skip because this entity doesn't match our filter
            continue osmEntityLoop;
          }
        }

        try {
          Geometry geom = fullyInside ?
              osmEntity.getGeometry(timestamp, tagInterpreter) :
              osmEntity.getGeometryClipped(timestamp, tagInterpreter, boundingBox);

          if (geom == null) throw new NotImplementedException(); // todo: fix this hack!
          if (geom.isEmpty()) throw new NotImplementedException(); // todo: fix this hack!
          //if (!(geom.getGeometryType() == "Polygon" || geom.getGeometryType() == "MultiPolygon")) throw new NotImplementedException(); // hack! // todo: wat?

          oshResult.put(timestamp, new ImmutablePair<>(osmEntity, geom));
        } catch (NotImplementedException err) {
          // todo: what to do here???
        } catch (IllegalArgumentException err) {
          System.err.printf("Relation %d skipped because of invalid geometry at timestamp %d\n", osmEntity.getId(), timestamp);
        } catch (TopologyException err) {
          System.err.printf("Topology error at object %d at timestamp %d: %s\n", osmEntity.getId(), timestamp, err.toString());
        }
      }

      // add skipped timestamps (where nothing has changed from the last timestamp) to set of results
      for (Map.Entry<Long, List<Long>> entry : queryTs.entrySet()) {
        Long key = entry.getKey();
        if (oshResult.containsKey(key)) { // could be missing in case this version
          Pair<OSMEntity, Geometry> existingResult = oshResult.get(key);
          for (Long additionalTs : entry.getValue()) {
            oshResult.put(additionalTs, existingResult);
          }
        }
      }

      results.add(oshResult);
    }

    // return as an obj stream
    return results.stream();
  }

}