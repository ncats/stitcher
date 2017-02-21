package ncats.stitcher;

import org.neo4j.graphdb.Label;

/**
 * Auxilary node types
 */
public enum AuxNodeType implements Label {
    SNAPSHOT,
    SINGLETON,
    GROUP,
    STITCHED,
    COMPONENT,
    DATA,
    ENTITY,
    DATASOURCE,
    BLACKLIST
}
