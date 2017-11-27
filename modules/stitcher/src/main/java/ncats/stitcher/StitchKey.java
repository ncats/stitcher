package ncats.stitcher;

import java.util.Set;
import java.util.EnumSet;
import org.neo4j.graphdb.RelationshipType;

/**
 * Stitching properties
 */
public enum StitchKey implements RelationshipType {
    /*
     * Name
     */
    N_Name(2), // any name

    /*
     * Compound identifiers
     */
    I_UNII(3), // FDA UNII
    I_CAS(2), // CAS registry number
    I_SID(Long.class), // pubchem sid
    I_CID(2, Long.class), // public cid
    I_ChEMBL(2), // CHEMBL_ID
    I_DB(2), // DrugBank
    I_CODE, // any code

    /*
     * Other identifiers
     */
    I_MeSH(2),
    I_UniProt(2), // UniProt id
    I_NCT, // clinical trial NCT
    I_PMID(2, Long.class), // PubMed id
    I_ANY(Long.class), // Any numeric id
    
    /*
     * Compound hash
     */
    H_InChIKey(3), // InChIKey
    H_LyChI_L1, // LyChI Layer 1
    H_LyChI_L2, // LyChI layer 2
    H_LyChI_L3, // LyChI layer 3
    H_LyChI_L4(2), // LyChI layer 4
    H_LyChI_L5(3), // LyChI layer 4 with salt + solvent

    /*
     * Generic hash
     */
    H_SHA1(5), // SHA1 hash
    H_SHA256(5), // SHA256 hash
    H_MD5(4), // MD5 hash

    /*
     * URL
     */
    U_Wikipedia, // Wikipedia URL
    U_DOI,  // DOI URL
    
    /*
     * Tag
     */
    T_ActiveMoiety(5, true), // active moiety relationship (directed)
    T_Keyword // Keyword
    ;
  
    final public Class type;
    final public int priority; // priority 1 (lowest) to 5 (highest)
    final public boolean directed;
    
    StitchKey () {
        this (1, String.class, false);
    }
    StitchKey (int priority) {
        this (priority, String.class, false);
    }
    StitchKey (int priority, Class type) {
        this (priority, type, false);
    }
    StitchKey (int priority, boolean directed) {
        this (priority, String.class, directed);
    }
    StitchKey (Class type) {
        this (1, type, false);
    }
    StitchKey (Class type, boolean directed) {
        this (1, type, directed);
    }
    StitchKey (int priority, Class type, boolean directed) {
        this.type = type;
        this.priority = priority;
        this.directed = directed;
    }

    // return StitchKey based on the priority range (inclusive)
    public static StitchKey[] keys (int lower, int upper) {
        Set<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
        for (StitchKey k : EnumSet.allOf(StitchKey.class)) {
            if ((lower <= 0 || k.priority >= lower)
                && (upper <= 0 || k.priority <= upper))
                keys.add(k);
        }
        
        return keys.toArray(new StitchKey[0]);
    }

    public static StitchKey[] keys (Class cls) {
        Set<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
        for (StitchKey k : EnumSet.allOf(StitchKey.class))
            if (cls.isAssignableFrom(k.type))
                keys.add(k);
        return keys.toArray(new StitchKey[0]);
    }
}
