package ncats.stitcher;

import org.neo4j.graphdb.Label;

/**
 * Auxilary node types
 */
public enum AuxNodeType implements Label {
    SNAPSHOT,
    SINGLETON,
    SGROUP,
    COMPONENT,
    DATA,
    ENTITY,
    DATASOURCE,
    BLACKLIST
}
