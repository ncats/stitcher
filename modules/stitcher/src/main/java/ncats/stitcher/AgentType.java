package ncats.stitcher;

/**
 * Agents primarily classified by level of development
 * Not an ingredient or API or drug substance.
 * This is a higher-level term that connotes the essential actor part of that substance akin to 'active moiety'
 */
public enum AgentType {
    ApprovedDrug ("Agent with marketing authorization as a treatment for an indication."),
    
    InvestigationalDrug ("Agent has been used clinically in man (has/had IND), but without marketing authorization or similar approval"),
    
    Candidate ("Preclinical agent has passed clinical selection, in clinical development"),
    
    Probe ("A preclinical tool (lead)"),
    
    // RNAi,   wouldn't RNAi be either a drug or a probe depending on its development?
    
    Other ("Vet-approved drug? inactive ingredient from approved drug?")

    ;

    final String desc;
    AgentType (String desc) {
        this.desc = desc;
    }
    public String desc () { return desc; }
}
