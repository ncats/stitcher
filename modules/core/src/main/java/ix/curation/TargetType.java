package ix.curation;

public enum TargetType { // TargetBiology types
    MolecularTarget, // a protein or other molecule that physically interacts with an agent
    TargetFamily, // an abstract collection of protein targets
    TargetClass, // An entry in the classification hierarchy that is really just a label
    MechanismOfAction, // conceptual biology entity that is affected by an agent, such as a pathway, a target organ, generic process like cholesterol metabolism
    Other // I sure there is something that'll trip us up
}
