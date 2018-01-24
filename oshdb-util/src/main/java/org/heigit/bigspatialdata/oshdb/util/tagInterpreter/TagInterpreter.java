package org.heigit.bigspatialdata.oshdb.util.tagInterpreter;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import org.heigit.bigspatialdata.oshdb.osh.OSHWay;
import org.heigit.bigspatialdata.oshdb.osm.OSMEntity;
import org.heigit.bigspatialdata.oshdb.osm.OSMMember;
import org.heigit.bigspatialdata.oshdb.osm.OSMNode;
import org.heigit.bigspatialdata.oshdb.osm.OSMRelation;
import org.heigit.bigspatialdata.oshdb.osm.OSMType;
import org.heigit.bigspatialdata.oshdb.osm.OSMWay;

/**
 * instances of this class are used to determine whether a OSM way represents a polygon or linestring geometry.
 */
public class TagInterpreter implements Serializable {
  protected int areaNoTagKeyId, areaNoTagValueId;
  protected Map<Integer, Set<Integer>> wayAreaTags;
  protected Map<Integer, Set<Integer>> relationAreaTags;
  protected Set<Integer> uninterestingTagKeys;
  protected int outerRoleId, innerRoleId, emptyRoleId;

  public TagInterpreter(
      int areaNoTagKeyId,
      int areaNoTagValueId,
      Map<Integer, Set<Integer>> wayAreaTags,
      Map<Integer, Set<Integer>> relationAreaTags,
      Set<Integer> uninterestingTagKeys,
      int outerRoleId,
      int innerRoleId,
      int emptyRoleId
  ) {
    this.areaNoTagKeyId = areaNoTagKeyId;
    this.areaNoTagValueId = areaNoTagValueId;
    this.wayAreaTags = wayAreaTags;
    this.relationAreaTags = relationAreaTags;
    this.uninterestingTagKeys = uninterestingTagKeys;
    this.outerRoleId = outerRoleId;
    this.innerRoleId = innerRoleId;
    this.emptyRoleId = emptyRoleId;
  }

  private boolean evaluateWayForArea(OSMWay entity) {
    int[] tags = entity.getTags();
    if (entity.hasTagValue(areaNoTagKeyId, areaNoTagValueId))
      return false;
    for (int i = 0; i < tags.length; i += 2) {
      if (wayAreaTags.containsKey(tags[i]) &&
          wayAreaTags.get(tags[i]).contains(tags[i + 1]))
        return true;
    }
    return false;
  }

  private boolean evaluateRelationForArea(OSMRelation entity) {
    int[] tags = entity.getTags();
    // skip area=no check, since that doesn't make much sense for multipolygon relations (does it??)
    for (int i = 0; i < tags.length; i += 2) {
      if (relationAreaTags.containsKey(tags[i]) &&
          relationAreaTags.get(tags[i]).contains(tags[i + 1]))
        return true;
    }
    return false;
  }

  public boolean isArea(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    } else if (entity instanceof OSMWay) {
      OSMWay way = (OSMWay) entity;
      OSMMember[] nds = way.getRefs();
      // must form closed ring with at least 3 vertices
      if (nds.length < 4 || nds[0].getId() != nds[nds.length - 1].getId()) {
        return false;
      }
      return this.evaluateWayForArea((OSMWay)entity);
    } else /*if (entity instanceof OSMRelation)*/ {
      return this.evaluateRelationForArea((OSMRelation)entity);
    }
  }

  public boolean isLine(OSMEntity entity) {
    if (entity instanceof OSMNode) {
      return false;
    }
    return !isArea(entity);
  }

  public boolean hasInterestingTagKey(OSMEntity osm) {
    int[] tags = osm.getTags();
    for (int i=0; i<tags.length; i+=2) {
      if (!uninterestingTagKeys.contains(tags[i]))
        return true;
    }
    return false;
  }

  public boolean isOldStyleMultipolygon(OSMRelation osmRelation) {
    int outerWayCount = 0;
    OSMMember[] members = osmRelation.getMembers();
    for (int i=0; i<members.length; i++) {
      if (members[i].getType() == OSMType.WAY && members[i].getRoleId() == outerRoleId)
        if (++outerWayCount > 1) return false; // exit early if two outer ways were already found
    }
    if (outerWayCount != 1) return false;
    int[] tags = osmRelation.getTags();
    for (int i=0; i<tags.length; i+=2) {
      if (relationAreaTags.containsKey(tags[i]) && relationAreaTags.get(tags[i]).contains(tags[i+1]))
        continue;
      if (!uninterestingTagKeys.contains(tags[i]))
        return false;
    }
    return true;
  }

  public boolean isMultipolygonOuterMember(OSMMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) return false;
    int roleId = osmMember.getRoleId();
    return roleId == this.outerRoleId ||
           roleId == this.emptyRoleId; // some historic osm data may still be mapped without roles set -> assume empty roles to mean outer
    // todo: check if there is need for some more clever outer/inner detection for the empty role case with old data
  }

  public boolean isMultipolygonInnerMember(OSMMember osmMember) {
    if (!(osmMember.getEntity() instanceof OSHWay)) return false;
    return osmMember.getRoleId() == this.innerRoleId;
  }

}