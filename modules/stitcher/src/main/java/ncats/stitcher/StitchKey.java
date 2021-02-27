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
    I_UNII(4), // FDA UNII
    I_CAS(2), // CAS registry number
    I_SID(2, Long.class), // pubchem sid
    I_CID(2, Long.class), // pubchem cid
    I_ChEMBL(2), // CHEMBL_ID
    I_DB(2), // DrugBank
    
    I_CODE(2), // any code

    /*
     * Other identifiers
     */
    I_MeSH(2),
    I_UniProt(2), // UniProt id
    I_NCT, // clinical trial NCT
    I_PMID(2, Long.class), // PubMed id
    I_ANY(Long.class), // Any numeric id
    I_GENE, // gene id (e.g., HGNC:10002, OMIM:603894) or symbol (e.g., RGS6) 
    
    /*
     * Compound hash
     */
    H_InChIKey(3), // InChIKey
    H_LyChI_L1(1), // LyChI Layer 1
    H_LyChI_L2(1), // LyChI layer 2
    H_LyChI_L3(1), // LyChI layer 3
    H_LyChI_L4(3), // LyChI layer 4
    H_LyChI_L5(4), // LyChI layer 4 with salt + solvent

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
     * Ontology relationships
     */
    R_subClassOf(1, true),
    R_subPropertyOf(1, true),
    R_equivalentClass(5),
    R_exactMatch(5),
    R_closeMatch(4),
    R_relatedTo(2),
    R_axiom(3, true),

    /*
     * UMLS relationship attributes
     * https://www.nlm.nih.gov/research/umls/knowledge_sources/metathesaurus/release/abbreviations.html
     */
    R_rel,

    /*
     * has phenotype relationship
     */
    R_hasPhenotype(3, true),

    /*
     * Biology relationships
     */
    R_activeMoiety(5, true), // active moiety relationship (directed)
    R_ppi, // Protein-protein interaction; directionality prey -> bait if known
    
    /*
     * Tag
     */
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
    StitchKey (int priority, boolean directed) {
        this (priority, String.class, directed);
    }
    StitchKey (Class type) {
        this (1, type, false);
    }
    StitchKey (int priority, Class type) {
        this (priority, type, false);
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
