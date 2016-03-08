package ix.curation;

import org.neo4j.graphdb.RelationshipType;

/**
 * auxilary relationship
 */
public enum AuxRelType implements RelationshipType {
    PAYLOAD,
    CC // UnionFind connected component
}
