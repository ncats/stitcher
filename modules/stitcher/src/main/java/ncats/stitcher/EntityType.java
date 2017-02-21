package ncats.stitcher;

import org.neo4j.graphdb.Label;

/**
 * These are the node label
 */
public enum EntityType implements Label {
    Condition ("Disease or other medical condition like pregnancy"),
    
    Agent ("A drug, probe or other agent; conceptually equivalent to the active moiety of a drug substance; see AgentType"),
    
    TargetBiology ("Molecular target or biological process that is the object of affection by an agent; ActionType (pharmacology) links TargetBiology and Agent"),
    
    ClinicalTrial ("Clinical trial entity"),
    Other ("Generic entity")
    ;

    final String desc;
    EntityType (String desc) {
        this.desc = desc;
    }
    public String desc () { return desc; }
}
