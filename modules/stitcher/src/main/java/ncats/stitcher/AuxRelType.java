package ncats.stitcher;

import org.neo4j.graphdb.RelationshipType;

/**
 * auxilary relationship
 */
public enum AuxRelType implements RelationshipType {
    PAYLOAD,
    STITCH,
    EVENT,
    SUMMARY,
    SELF
}
