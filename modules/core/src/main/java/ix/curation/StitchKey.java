package ix.curation;

import org.neo4j.graphdb.RelationshipType;

/**
 * Stitching properties
 */
public enum StitchKey implements RelationshipType {
    /*
     * Name
     */
    N_Name, // any name

    /*
     * Identifier
     */
    I_UNII(2), // FDA UNII
    I_CAS(1), // CAS registry number
    I_SID(1, Long.class), // pubchem sid
    I_CID(2, Long.class), // public cid
    I_NCT(1), // clinical trial NCT
    I_PMID(2, Long.class), // PubMed id
    I_UniProt(2), // UniProt id
    I_ChEMBL(2), // CHEMBL_ID
    I_Code(1), // any code
    I_Any(1, Long.class), // Any numeric id

    /*
     * Hash
     */
    H_InChIKey(3), // InChIKey
    H_LyChI_L1, // LyChI Layer 1
    H_LyChI_L2, // LyChI layer 2
    H_LyChI_L3, // LyChI layer 3
    H_LyChI_L4(2), // LyChI layer 4
    H_LyChI_L5(3), // LyChI layer 4 with salt + solvent
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
    T_Keyword // Keyword
    ;
  
    final public Class type;
    final public int priority; // priority -5 (lowest) to 5 (highest)
    StitchKey () {
        this (0, String.class);
    }
    StitchKey (int priority) {
        this (priority, String.class);
    }
    StitchKey (Class type) {
        this (0, type);
    }
    StitchKey (int priority, Class type) {
        this.type = type;
        this.priority = priority;
    }
}
