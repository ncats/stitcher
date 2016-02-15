package ix.curation;

import org.neo4j.graphdb.RelationshipType;

/**
 * Stitching properties
 */
public enum StitchKey implements RelationshipType {
    /*
     * This should be treated as strongest possible
     */
    STITCHED,
    
    /*
     * Name
     */
    N_Official, // official name
    N_Systematic, // systematic Name
    N_Brand, // brand name
    N_Common, // common name
    N_INN, // INN name
    N_USAN, // USAN name
    N_Code, // code name
    N_Synonym, // synonym
    N_Name, // any name

    /*
     * Identifier
     */
    I_UNII, // FDA UNII
    I_CAS, // CAS registry number
    I_SID(Long.class), // pubchem sid
    I_CID(Long.class), // public cid
    I_NCT, // clinical trial NCT
    I_PMID(Long.class), // PubMed id
    I_UniProt, // UniProt id
    I_ChEMBL, // CHEMBL_ID
    I_Any, // Any generic id

    /*
     * Hash
     */
    H_InChIKey, // InChIKey
    H_LyChI_L1, // LyChI Layer 1
    H_LyChI_L2, // LyChI layer 2
    H_LyChI_L3, // LyChI layer 3
    H_LyChI_L4, // LyChI layer 4
    H_LyChI_L5, // LyChI layer 4 with salt + solvent
    H_SHA1, // SHA1 hash
    H_SHA256, // SHA256 hash
    H_MD5, // MD5 hash

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
  
    final Class type;
    StitchKey () {
        this (String.class);
    }
    StitchKey (Class type) {
        this.type = type;
    }

    public Class getType () { return type; }
}
