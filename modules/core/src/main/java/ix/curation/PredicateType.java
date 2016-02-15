package ix.curation;

import org.neo4j.graphdb.RelationshipType;

/**
 * Generic predicates describing relationships
 */
public enum PredicateType implements RelationshipType {
    ActiveMoiety, // SRS pointers to active moiety uniis
    HasComponent, // SRS pointer for mixtures
    ConceptOf, // SRS pointer for specified substances (concepts)
    Interacts,
    IsA,
    Has,
    SameAs,
    Contains,
    RelatedTo,
    ParentOf,
    ChildOf,
    Stitch,
    Obsolete,
    Update,
    Other
}
