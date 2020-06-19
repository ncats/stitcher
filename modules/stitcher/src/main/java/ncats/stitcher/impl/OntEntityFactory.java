package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/**
 * sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.OntEntityFactory ordo.db ordo.owl"
 */
public class OntEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(OntEntityFactory.class.getName());

    static final int DEBUG = 0;

    /*
     * minimum length for xref
     */
    static final int MIN_XREF_LENGTH = 5;
    
    static final String[] _RELATIONS = {
        "subPropertyOf",
        "subClassOf",
        "excluded_subClassOf",
        "intersectionOf",
        "equivalentClass",
        "exactMatch",
        "closeMatch",
        "manifestation_of",
        "has_phenotype",
        "allelic_variant_of",
        "has_allelic_variant",
        "has_inheritance_type",
        "inheritance_type_of",
        "phenotype_of",
        "part_of",
        "mapped_to",
        "mapped_from",
        "related_to",
        "isa",
        "inverse_isa",
        "has_dose_form",
        "has_ingredient",
        "has_go_association",
        "form_of",
        "precise_ingredient_of",
        "constitutes",
        "has_tradename",
        "tradename_of",
        "consists_of",
        "has_doseformgroup",
        "has_part",
        "ingredient_of",
        "has_quantified_form",
        "reformulated_to",
        "reformulation_of",
        "contains",
        "contained_in",
        "has_form",
        "quantified_form_of",
        "doseformgroup_of",
        "IAO_0100001", // term_replaced_by
        "SIB"
    };
    static final Set<String> RELATIONS =
        new TreeSet<>(Arrays.asList(_RELATIONS));

    static class OntologyResource {
        final public Resource resource;
        public String uri;
        final public String type;
        final public boolean anonymous;
        final public Map<String, Object> props = new TreeMap<>();
        final public Map<String, Object> links = new TreeMap<>();
        final public List<OntologyResource> axioms = new ArrayList<>();
        
        OntologyResource (Resource res) {
            String t = null;
            for (StmtIterator it = res.listProperties(); it.hasNext(); ) {
                Statement stm = it.next();
                Property prop = stm.getPredicate();
                RDFNode obj = stm.getObject();
                String pname = prop.getLocalName();
                if (obj.isResource()) {
                    Resource r = obj.asResource();
                    if ("type".equals(pname)) {
                        t = r.getLocalName();
                    }
                    Object old = links.get(pname);
                    links.put(pname, old != null ? Util.merge(old, r) : r);
                }
                else {
                    switch (pname) {
                    case "OGG_0000000029": // sigh
                        { List<Resource> gores = new ArrayList<>();
                            for (String tok : obj.toString().split(";")) {
                                String go = tok.trim();
                                if (go.startsWith("GO_")) {
                                    int pos = go.indexOf(' ');
                                    if (pos > 0)
                                        go = go.substring(0, pos);
                                    gores.add
                                        (res.getModel().createResource
                                         ("http://purl.obolibrary.org/obo/"+go));
                                }
                            }
                            links.put("has_go_association",
                                      gores.toArray(new Resource[0]));
                        }
                        break;
                        
                    default: 
                        try {
                            Object v = obj.asLiteral().getValue();
                            if (!v.getClass().isAssignableFrom(Number.class))
                                v = v.toString();
                            if (!"".equals(v)) {
                                Object old = props.get(pname);
                                props.put(pname, old != null
                                          ? Util.merge(old, v) : v);
                            }
                        }
                        catch (Exception ex) {
                            logger.log
                                (Level.SEVERE, "Can't literal for "+pname, ex);
                        }
                    }
                }
            }

            this.uri = getURI (res);
            if (this.uri == null && "Class".equalsIgnoreCase(t)) {
                this.uri = res.toString();
                this.anonymous = true;
            }
            else
                this.anonymous = false;
            this.type = t;
            this.resource = res;
        }

        public int hashCode () { return resource.hashCode(); }
        public boolean equals (Object obj) {
            if (obj instanceof OntologyResource) {
                return resource.equals(((OntologyResource)obj).resource);
            }
            return false;
        }

        public boolean isAxiom () { return "Axiom".equalsIgnoreCase(type); }
        public boolean isClass () { return "Class".equalsIgnoreCase(type); }
        public boolean isRestriction () {
            return "Restriction".equalsIgnoreCase(type);
        }
        public boolean isOntology () {
            return "Ontology".equalsIgnoreCase(type);
        }

        public String toString () {
            StringBuilder sb = new StringBuilder ();
            sb.append("> ");
            if (isClass()) sb.append(resource.getLocalName()+" "+uri);
            else if (isAxiom()) sb.append("Axiom "+resource.getId());
            else sb.append(type+" "+resource.toString());
            sb.append("\n...properties\n");
            toString (sb, props);
            sb.append("...links\n");
            toString (sb, links);
            return sb.toString();
        }

        void toString (StringBuilder sb, Map<String, Object> values) {
            for (Map.Entry<String, Object> me : values.entrySet()) {
                sb.append("-- "+me.getKey()+":");
                Object value = me.getValue();
                if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); ++i) {
                        Object v = Array.get(value, i);
                        sb.append(" "+v+"["+v.getClass().getSimpleName()+"]");
                    }
                }
                else {
                    sb.append(" "+value+"["
                              +value.getClass().getSimpleName()+"]");
                }
                sb.append("\n");
            }
        }
    }

    Map<Resource, OntologyResource> resources = new LinkedHashMap<>();
    Map<Resource, OntologyResource> xrefs = new LinkedHashMap<>();
    OntologyResource ontology;
    
    public OntEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public OntEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public OntEntityFactory (File dir) throws IOException {
        super (dir);
    }

    static boolean isDeferred (String field) {
        return RELATIONS.contains(field);
    }

    @Override
    protected void init () {
        super.init();
        setIdField (Props.URI);
        setNameField ("label");
        setStrucField ("smiles");
        add (N_Name, "label")
            .add(N_Name, "altLabel")
            .add(N_Name, "hasExactSynonym")
            .add(N_Name, "hasRelatedSynonym")
            .add(N_Name, "IAO_0000118") // alternative term
            .add(N_Name, "P90") // synonym
            .add(N_Name, "P107") // display name
            .add(N_Name, "P108") // preferred name
            .add(I_CODE, "hasDbXref")
            .add(I_CODE, "id")
            .add(I_CODE, "notation")
            .add(I_CODE, "hasAlternativeId")
            // Contains a Concept Unique Identifier for those concepts that
            //  appear in NCI Metathesaurus but not in the NLM UMLS
            .add(I_CODE, "P208")
            .add(I_CODE, "P215") // KEGG
            .add(I_CODE, "P332") // MGI
            .add(I_CODE, "P368") // CHEBI
            .add(I_CODE, "P369") // HGNC
            .add(I_CODE, "P387") // GO
            .add(I_CODE, "SY")
            .add(I_CODE, "RQ")
            .add(I_CODE, "cui")
            .add(I_UNII, "P319")
            .add(I_GENE, "GENESYMBOL")
            .add(I_GENE, "OGG_0000000004")
            .add(I_GENE, "IAO_0000118")
            .add(I_GENE, "P321")
            //.add(I_PMID, "OGG_0000000030")
            //.add(I_PMID, "P171")
            .add(I_CAS, "CAS")
            .add(I_CAS, "P210")
            .add(H_InChIKey, "inchikey")
            .add(T_Keyword, "inSubset")
            .add(T_Keyword, "type")
            .add(T_Keyword, "hasOBONamespace")
            .add(T_Keyword, "hasSTY")
            .add(T_Keyword, "P106") // nci thesaurus
            .add(T_Keyword, "P386") // source name
            ;
        graphDb.createIndex(AuxNodeType.DATA, "id");
        graphDb.createIndex(AuxNodeType.DATA, "notation");
        graphDb.createIndex(AuxNodeType.DATA, "uri");
    }

    protected void reset () {
        ontology = null;
        resources.clear();
        xrefs.clear();
    }
    
    static String getResourceValue (Resource r) {
        String v = r.getLocalName();
        return v != null ? v : getURI (r);
    }

    static String getURI (Resource r) {
        String uri = r.getURI();
        // map http://purl.bioontology.org/ontology/MESH/D014406
        // to http://purl.obolibrary.org/obo/MESH_D014406
        // so as to match MONDO reference
        if (uri != null
            && uri.startsWith("http://purl.bioontology.org/ontology/MESH/")) {
            String[] toks = uri.split("/");
            uri = "http://purl.obolibrary.org/obo/MESH_"+toks[toks.length-1];
        }
        else if (uri == null) {
            //uri = r.toString();
        }
        return uri;
    }

    protected String transform (String value) {
        if (value.startsWith("UMLS_CUI")) {
            value = value.replaceAll("_CUI", "");
        }
        else if (value.startsWith("UniProtKB:")) {
            int pos = value.indexOf('-');
            if (pos > 0)
                // UniProtKB:P52198-1 => UniProtKB:P52198
                value = value.substring(0, pos);
        }
        return value;
    }

    protected static Object map (Object value, Function<Object, Object> f) {
        if (value == null) {
        }
        else if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i) {
                Array.set(value, i, map (Array.get(value, i), f));
            }
        }
        else {
            value = f.apply(value);
        }
        return value;
    }

    protected static Object filter (Object value, Predicate predicate) {
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            List vals = new ArrayList ();
            Class type = null;
            for (int i = 0; i < len; ++i) {
                Object v = Array.get(value, i);
                if (predicate.test(v)) {
                    if (type == null)
                        type = v.getClass();
                    vals.add(v);
                }
            }
            
            if (!vals.isEmpty()) {
                value = Array.newInstance(type, vals.size());
                for (int i = 0; i < vals.size(); ++i)
                    Array.set(value, i, vals.get(i));
            }
            else {
                value = null;
            }
        }
        else if (predicate.test(value)) {
        }
        else {
            value = null;
        }
        return value;
    }

    /*
     * TODO: NEED TO PUT ALL THESE RULES IN A CONFIG FILE!
     */
    protected Map<String, Object> sigh (Map<String, Object> data) {
        // don't use ICD code for stitching
        List<String> xrefs = new ArrayList<>();
        List<String> icds = new ArrayList<>();
        List<String> others = new ArrayList<>();
        List<String> useful = new ArrayList<>();
        
        Object value = data.remove("hasDbXref");
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(value, i);
                    if (v instanceof Resource) {
                        others.add(getURI ((Resource)v));
                    }
                    else {
                        String s = v.toString();
                        if ("".equals(s) || s.length() < MIN_XREF_LENGTH);
                        else if (s.startsWith("ICD"))
                            icds.add(s);
                        else
                            xrefs.add(transform (s));
                    }
                }
            }
            else if (value instanceof Resource) {
                others.add(getURI ((Resource)value));
            }
            else {
                String v = value.toString();
                if ("".equals(v) || v.length() < MIN_XREF_LENGTH);
                else if (v.startsWith("ICD")) icds.add(v);
                else xrefs.add(transform (v));
            }
        }

        Object obj = data.remove("prefLabel");
        if (obj != null)
            data.put("label", obj);

        for (String p : new String[]{"mapped_to", "related_to", "cui",
                                     "SY", "RQ"}) {
            obj = data.remove(p);
            if (obj != null) {
                data.put(p, map (obj, a -> "UMLS:"+a));
            }
        }
        
        obj = data.get("hasRelatedSynonym");
        if (obj != null) {
            obj = filter (obj, p -> {
                    String s = (String)p;
                    return s.length() >= MIN_XREF_LENGTH;
                });
            if (obj != null)
                data.put("hasRelatedSynonym", obj);
        }

        if (ontology == null) {
        }
        else if (ontology.resource != null
                 && "Thesaurus.owl".equals(ontology.resource.getLocalName())) {
            // NCI Thesaurus
            obj = data.get("NHC0");
            if (obj != null) {
                xrefs.add("NCIT:"+obj);
                xrefs.add("NCI:"+obj);
            }
            obj = data.get("P100");
            if (obj != null)
                xrefs.add("OMIM:"+obj);
            obj = data.get("P175");
            if (obj != null)
                xrefs.add("NSC:"+obj);
            obj = data.get("P207");
            if (obj != null)
                xrefs.add("UMLS:"+obj);
            obj = data.get("P321");
            if (obj != null)
                xrefs.add("GENE:"+obj);
            obj = data.get("P93");
            if (obj != null)
                xrefs.add("UNIPROTKB:"+obj);
        }
        else if (ontology.resource != null
                 && "cl.owl".equals(ontology.resource.getLocalName())) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.startsWith("OBOL:")
                    || u.startsWith("PRO:")
                    || u.startsWith("GOC:")
                    || u.equals("FMA:TA")
                    || u.equals("UBERON:CJM")
                    || u.startsWith("MGI:")
                    || u.startsWith("PATOC:")
                    || u.startsWith("NPX:")
                    || u.startsWith("ISBN:")
                    || u.equals("FB:MA")
                    || u.startsWith("SGD:")
                    || u.equals("AEO:JB")
                    || u.equals("GO:TFM")
                    || u.equals("CL:CVS")
                    || u.equals("CL:TM")
                    || u.equals("MA:TH")
                    || u.equals("VSAO:NI")
                    || u.startsWith("PHENOSCAPE:")
                    || u.startsWith("TAIR:")
                    || u.startsWith("WORDNET")
                    || u.startsWith("GC_ID:")
                    || u.startsWith("GO_REF:")
                    || u.equals("CHEBI")
                    || u.startsWith("ZFIN:")
                    || u.indexOf("IMMGEN.ORG") > 0) {
                    others.add(x);
                }
                else {
                    useful.add(x);
                }
            }  
        }
        else if (ontology.resource != null
                 && "clo.owl".equals(ontology.resource.getLocalName())) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.startsWith("GC_ID:")
                    || u.indexOf("CELLRESOURCE.CN") > 0) {
                    others.add(x);
                }
                else {
                    useful.add(x);
                }
            }            
        }
        else if (ontology.resource != null
                 && "bto.owl".equals(ontology.resource.getLocalName())) {
            final String[] BOGUS = {
                "DORLANDS_MEDICAL_DICTIONARY:MERCKSOURCE",
                "ATCC_AMERICAN_CELL_TYPE_CULTURE_COLLECTION:HTTP://WWW.LGCSTANDARDS-ATCC.ORG/",
                "WIKIPEDIA:THE_FREE_ENCYCLOPEDIA",
                "DEUTSCHE_SAMMLUNG_VON_MIKROORGANISMEN_UND_ZELLKULTUREN_GMBH:DSMZ",
                "THE_AMERICAN_HERITAGE_DICTIONARY_OF_THE_ENGLISH_LANGUAGE:FOURTH_EDITION._2000.",
                "FAST_HEALTH_MEDICAL_DICTIONARY:HTTP://WWW.FASTHEALTH.COM/DICTIONARY/",
                "MEDICAL_DICTIONARY:HTTP://MEDICAL-DICTIONARY.THEFREEDICTIONARY.COM/",
                "HYPERCLDB:HTTP://WWW.BIOTECH.IST.UNIGE.IT/",
                "ONLINE_MEDICAL_DICTIONARY:HTTP://CANCERWEB.NCL.AC.UK/",
                "MEDICAL_DICTIONARY:HTTP://WWW.MEDTERMS.COM/",
                "DORLANDS_MEDICAL_DICTIONARY:MERCKMEDICUS",
                "THE_DICTIONARY_OF_CELL_AND_MOLECULAR_BIOLOGY:THIRD_EDITION",
                "MEDICAL_DICTIONARY_ONLINE:HTTP://WWW.ONLINE-MEDICAL-DICTIONARY.ORG/",
                "DICTIONARY:HTTP://WWW.THEFREEDICTIONARY.COM/",
                "PAE_VIRTUAL_GLOSSARY:PLANTS,_ANIMALS_AND_THE_ENVIRONMENT_GLOSSARY_DERIVED_FROM_LEADING_WCB/MCGRAW-HILL_TEXTBOOKS",
                "BIOLOGY-ONLINE_DICTIONARY:HTTP://WWW.BIOLOGY-ONLINE.ORG/DICTIONARY/",
                "EUROPEAN_COLLECTION_OF_CELL_CULTURES:ECACC",
                "MEDICAL_DICTIONARY:HTTP://WWW.MEDILEXICON.COM/",
                "PMID:11595720",
                "CURATORS:MGR",
                "CANCER.GOV_DICTIONARY:HTTP://WWW.NCI.NIH.GOV/DICTIONARY",
                "PMID:17760834",
                "PMID:14504097",
                "DICTIONARY_OF_ICHTHYOLOGY:HTTP://WWW.BRIANCOAD.COM/DICTIONARY/INTRODUCTION.HTM",
                "CLS-CELL_LINES_SERVICE:HTTP://WWW.CELL-LINES-SERVICE.DE/",
                "MONDOFACTO_DICTIONARY:HTTP://WWW.MONDOFACTO.COM/FACTS/DICTIONARY?",
                "PMID:15610063",
                "CELL_NAME_INDEX:HTTP://WWW.JHSF.OR.JP/BANK/CELLNAME.HTML",
                "PMID:21465477",
                "PMID:7693337",
                "DICTIONARY_OF_CANCER_TERMS:HTTP://WWW.CANCER.GOV/",
                "ENCYCLOPEDIA:HTTP://ENCYCLOPEDIA.THEFREEDICTIONARY.COM/",
                "PMID:17123352",
                "ASTERAND_BIOSCIENCE:HTTP://SOLUTIONS.ASTERAND.COM/",
                "JAPANESE_COLLECTION_OF_RESEARCH_BIORESOURCES:HTTP://CELLBANK.NIBIO.GO.JP/",
                "COPE_CYTOKINES_&_CELLS_ONLINE_PATHFINDER_ENCYCLOPAEDIA:HTTP://WWW.COPEWITHCYTOKINES.DE/COPE.CGI",
                "RIKEN_BIORESOURCE_CENTER:HTTP://WWW2.BRC.RIKEN.JP/",
                "PMID:10673746",
                "ENCYCLOPEDIA_BRITANNICA:HTTP://WWW.BRITANNICA.COM/",
                "THE_AMERICAN_HERITAGE_DICTIONARY_OF_THE_ENGLISH_LANGUAGE:FOURTH_EDITION_COPYRIGHT_2000",
                "PMID:17332333",
                "ONLINE_DICTIONARY_OF_INVERTEBRATE_ZOOLOGY:HTTP://DIGITALCOMMONS.UNL.EDU/CGI/VIEWCONTENT.CGI?ARTICLE=1017&CONTEXT=ONLINEDICTINVERTZOOLOGY",
                "ENCYCLOPEDIA.COM:HTTP://WWW.ENCYCLOPEDIA.COM/",
                "WORMATLAS:HTTP://WWW.WORMATLAS.ORG/",
                "NCI_DICTIONARY_OF_CANCER_TERMS:HTTP://WWW.NCI.NIH.GOV/",
                "THE_COLUMBIA_ENCYCLOPEDIA:SIXTH_EDITION._2001",
                "PLANT_ANATOMY_GLOSSARY:HTTP://WWW.URI.EDU/CELS/BIO/PLANT_ANATOMY/GLOSSARY.HTML",
                "ONLINE_DICTIONARY_OF_INVERTEBRATE_ZOOLOGY:HTTP://DIGITALCOMMONS.UNL.EDU/ONLINEDICTINVERTZOOLOGY/",
                "MEDICAL_ENCYCLOPEDIA:HTTP://WWW.NLM.NIH.GOV/MEDLINEPLUS/ENCYCLOPEDIA.HTML",
                "REVIEW_GLOSSARY:HTTP://MEDINFO.UFL.EDU/",
                "THEFREEDICTIONARY:HTTP://WWW.THEFREEDICTIONARY.COM/",
                "WIKTIONARY:HTTPS://EN.WIKTIONARY.ORG/WIKI/",
                "HEALTH_PROTECTION_AGENCY_CULTURE_COLLECTIONS:HTTP://HPACULTURES.ORG.UK/PRODUCTS/CELLLINES/",
                "FEATHER_ANATOMY_AND_FUNCTION:HTTP://ANIMALS.ABOUT.COM",
                "ANSWERS.COM:HTTP://WWW.ANSWERS.COM/",
                "PLANT_BREEDING_DICTIONARY:HTTP://WWW.DESICCA.DE/PLANT_BREEDING/DICTIONARY/DICTIONARY_A/DICTIONARY_A.HTML",
                "ANATOMY_OF_OLFACTORY_SYSTEM:HTTP://WWW.EMEDICINE.COM/",
                "GRAMENE_DB:HTTP://DEV.GRAMENE.ORG/DB/ONTOLOGY/SEARCH/",
                "WICELL:WWW.WICELL.ORG/",
                "POLLINATION_AND_FLORAL_ECOLOGY:PAT_WILLMER",
                "THE_AMERICAN_HERITAGE_MEDICAL_DICTIONARY:COPYRIGHT_2007",
                "CELLBANK:HTTP://CELLBANK.NIBIOHN.GO.JP//LEGACY/CELLDATA/",
                "STEM_CELL_INFORMATION:HTTP://STEMCELLS.NIH.GOV/INFO/BASICS/BASICS4.ASP",
                "INVITROGEN:HTTP://PRODUCTS.INVITROGEN.COM/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=PRIMORDIUM",
                "PROMOCELL:HTTP://WWW.PROMOCELL.COM/",
                "NEUROLEX.ORG:HTTP://NEUROLEX.ORG/WIKI/CATEGORY:HIPPOCAMPAL_NEURON",
                "STARFISH_DIGESTION_AND_CIRCULATION:HTTP://WWW.VSF.CAPE.COM/_~_JDALE/SCIENCE/DIGEST.HTM",
                "JCRB_JAPANESE_COLLECTION_OF_RESEARCH_BIORESOURCES:HTTP://CELLBANK.NIBIO.GO.JP./",
                "CELLONTOLOGY:HTTPS://RAW.GITHUBUSERCONTENT.COM/OBOPHENOTYPE/CELL-ONTOLOGY/MASTER/CL.OBO",
                "DICTIONARY_OF_INVERTEBRATE_ZOOLOGY:HTTP://SPECIES-ID.NET/ZOOTERMS/",
                "THE_AMERICAN_HERITAGE_MEDICAL_DICTIONARY:2009",
                "ENCYCLOPAEDIA_BRITANNICA:HTTP://WWW.BRITANNICA.COM/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=NERVE+CORD",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.MERRIAM-WEBSTER.COM/DICTIONARY/ILIAC_ARTERY",
                "CELLS_FOR_MEDICAL_TREATMENT:HTTP://HSB.IITM.AC.IN/~JM/ARCHIVES/JULY-AUG05/ARTICLES_FILES/STEM.HTML",
                "WEBSTER'S_ONLINE_DICTIONARY:HTTP://WWW.WEBSTERS-ONLINE-DICTIONARY.ORG/",
                "DICTIONARY_OF_BIOLOGICAL_PSYCHOLOGY:PHILIP_WINN",
                "COPE_ENCYCLOPEDIA:HTTP://WWW.COPEWITHCYTOKINES.DE/COPE.CGI?KEY=MO7E",
                "BOTANY_AND_PALEOBOTANY_DICTIONARY:PLANT_GLOSSARY",
                "KERATIN.COM:HTTP://WWW.KERATIN.COM/",
                "GLOSSARY_OF_FERMENTATION_&_CELL_CULTURE_TERMS:HTTP://WWW.NBSC.COM/FILES/PAPERS/BP0600_GLOSS_36-44.PDF",
                "WEBSTER'S_REVISED_UNABRIDGED_DICTIONARY:1913",
                "DICTIONARY_OF_BOTANY:HTTP://BOTANYDICTIONARY.ORG/",
                "ARISTOTELES_UNIVERSITY_OF_THESSALONIKI_DICTIONARY_OF_MEDICAL_TERMS:HTTP://WWW.MED.AUTH.GR/DB/DICTIONARY1/GR/",
                "BRAIN_TUMOR_DICTIONARY:HTTP://WWW.VIRTUALTRIALS.COM/",
                "SAUNDERS_COMPREHENSIVE_VETERINARY_DICTIONARY:3RD_EDITION_2007_ELSEVIER",
                "THE_TREEDICTIONARY:HTTP://WWW.TREEDICTIONARY.COM/",
                "GARDENWEB_GLOSSARY_OF_BOTANICAL_TERMS:HTTP://GLOSSARY.GARDENWEB.COM/GLOSSARY/",
                "THE_NEW_DICTIONARY_OF_CULTURAL_LITERACY:THIRD_EDITION._2002.",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.MERRIAM-WEBSTER.COM/DICTIONARY/DERMATOME",
                "A_GUIDE_TO_BRAIN_ANATOMY:HTTP://WWW.WAITING.COM/BRAINANATOMY.HTML",
                "PRAWN:EXTERNAL_FEATURES_AND_LIFEHISTORY:HTTP://WWW.BIOLOGYDISCUSSION.COM/ZOOLOGY/PRAWN/",
                "GEODUCK_CLAM_(PANOPEA_ABRUPTA):_ANATOMY,_HISTOLOGY,_DEVELOPMENT,_PATHOLOGY,_PARASITES_AND_SYMBIONTS:HTTP://WWW-SCI.PAC.DFO-MPO.GC.CA/GEODUCK/HISTOVERVIEW_E.HTM",
                "LIFEMAPSCIENCES:HTTPS://DISCOVERY.LIFEMAPSC.COM/IN-VIVO-DEVELOPMENT/PLACENTA/EXTRAVILLOUS-CYTOTROPHOBLAST-LAYER/EXTRAVILLOUS-CYTOTROPHOBLAST-CELLS",
                "LIFECYCLE&PATHOLOGY_OF_PLASMODIUM_SPECIES:HTTP://WWW.MSU.EDU/COURSE/ZOL/316/PSPPBLOOD.HTM",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=EPITHELIUM",
                "ALBERTA_HERITAGE_FOUNDATION_OF_MEDICAL_RESEARCH:HTTP://WWW.AHFMR.AB.CA/HTA/HTA-PUBLICATIONS/REPORTS/INTRAOCULAR99/INTRAOCULAR.SHTML",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.MERRIAM-WEBSTER.COM/DICTIONARY/SUCKER",
                "PATENT_5766946_MONOCLONAL_ANTIBODIES_TO_GLYCOPROTEIN_P:HTTP://WWW.PATENTGENIUS.COM/PATENT/5766946.HTML",
                "ILLUSTRATED_ENCYCLOPEDIA_OF_HUMAN_ANATOMIC_VARIATION:PART_I_MUSCULAR_SYSTEM_HTTP://WWW.VH.ORG/ADULT/PROVIDER/ANATOMY/ANATOMICVARIANTS/MUSCULARSYSTEM/TERMINOLOGY.HTML",
                "DICTIONARY:HTTP://WWW.JANSEN.COM.AU/DICTIONARY_AC.HTML",
                "QBIOGENE:HTTP://WWW.QBIOGENE.COM/ADENOVIRUS/PRODUCTS/CELLLINES/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=SMOOTH+MUSCLE",
                "DICTIONARY_OF_GENOMICS_(JAPAN):HTTP://CNY.NEW21.NET/DICTIONARY/S-E.HTML",
                "DICTIONARY_OF_HUNTINGTON'S_DISEASE_(HD)_TERMS:HTTP://WWW.HDSA-WI.ORG/DICTIONARY.HTM",
                "BLATTELLA_GERMANICA_KURT_DOUGLAS_SALTZMANN_DISSERTATION:CHARACTERIZATION_OF_TERGAL_GLAND-SECRETED_PROTEINS_IN_THE_GERMAN_COCKROACH",
                "DICTIONARY_BIOLOGY-FORUMS:HTTP://BIOLOGY-FORUMS.COM/DEFINITIONS/INDEX.PHP?TITLE=SHOOT_MERISTEM",
                "WASHINGTON_BIOTECHNOLOGY:HTTP://WWW.WASHINGTONBIOTECH.COM/INFLAMMATION_MODELS/AIR_POUCH_MODEL.HTML",
                "GENEONTOLOGY:HTTP://OBO.CVS.SOURCEFORGE.NET/VIEWVC/OBO/OBO/ONTOLOGY/GENOMIC-PROTEOMIC/GENE_ONTOLOGY_EDIT.OBO",
                "DICTIONARY_OF_TROPICAL_MEDICINE:HTTP://TROPMED.ORG/DICTIONARY/",
                "THE_AMERICAN_HERITAGE_STEDMAN'S_MEDICAL_DICTIONARY:COPYRIGHT_2002",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=LYMPHOBLASTIC_LEUKEMIA",
                "SCIENCELL_RESEARCH_LABORATORIES:HTTP://WWW.SCIENCELLONLINE.COM/",
                "CODERS_ALMANAC_FOR_TERMINOLOGY:HTTP://WWW.CODINGBOOKS.COM/PDF/CDAT08.PDF",
                "DICTIONARY_OF_PSYCHOLOGY:HTTP://WWW.ENCYCLOPEDIA.COM/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=FORELIMB",
                "SYNTHESIS_AND_EVALUATION_OF_SELECTIVE_INHIBITORS_OF_ALDOSTERONE_SYNTHASE_(CYP11B2)_OF_THE_NAPHTHALENE_AND_DIHYDRONAPHTHALENE_TYPE_FOR_THE_TREATMENT_OF_CONGESTIVE_HEART_FAILURE_AND_MYOCARDIAL_FIBROSIS:DISSERTATION_ZUR_ERLANGUNG_DES_GRADES_DES_DOKTORS_DER_NATURWISSENSCHAFTEN_DER_NATURWISSENSCHAFTLICH-TECHNISCHEN_FAKULTAET_III_-CHEMIE,_PHARMAZIE,_BIO-_UND_WERKSTOFFWISSENSCHAFTEN-_DER_UNIVERSITAET_DES_SAARLANDES_MARIEKE_VOETS_SAARBRUECKEN_2006",
                "MOLLUSCS_GENERAL_ANATOMY:HTTP://WWW.EARTHLIFE.NET/INVERTS/MOLLUSCA.HTML",
                "MEDICAL_TERMINOLOGY_DICTIONARY:HTTP://MEDICAL.WEBENDS.COM/",
                "EMEDICINEHEALTH:HTTP://WWW.EMEDICINEHEALTH.COM/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=EPIDERMIS",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=VENTRICLE",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.MERRIAM-WEBSTER.COM/DICTIONARY/OLFACTORY_PLACODE",
                "CONCEPTS_IN_MYCOLOGY:HTTP://FACSTAFF.BLOOMU.EDU/CHAMURIS/TEXT/GLOSSARY.HTML",
                "ONLINE_MEDICAL_DICTIONARY:HTTP://WWW.IRISHHEALTH.COM/DICTIONARY.HTML?",
                "TAIR_THE_ARABIDOPSIS_INFORMATION_RESOURCE:HTTPS://WWW.ARABIDOPSIS.ORG/",
                "GLOSSARIUM_POLYGLOTTUM_BRYOLOIGIAE:HTTP://MOBOT.MOBOT.ORG/CGI-BIN/SEARCH_VAST?GLOSE=208",
                "HGCA_WHEAT_DISEASE_ENCYCLOPAEDIA:HTTP://WWW.HGCA.COM/",
                "MAIZE_DB:HTTP://WWW.MAIZEMAP.ORG/MMP_DOWNLOADS/POC/ZEA_MAYS_ANATOMY_ONTOLOGY_DEFINITIONS.TXT",
                "MARLIN:THE_MARINE_LIFE_INFORMATION_NETWORK",
                "LABOR&MORE:HTTP://WWW.LABORUNDMORE.COM/ARCHIVE/375929/NEUE-BIOKATALYSATOREN-AUS-BASIDIOMYCETEN.HTML",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.MERRIAM-WEBSTER.COM/DICTIONARYPARAMETRIUM",
                "GLOSSARY_OF_MYCOLOGICAL_TERMS:HTTP://WWW.MYCOLOGY.ADELAIDE.EDU.AU/VIRTUAL/GLOSSARY/",
                "FROM_MERRIAM-WEBSTER'S_ONLINE_DICTIONARY_AT_WWW.MERRIAM-WEBSTER.COM:HTTP://WWW.M-W.COM/CGI-BIN/DICTIONARY?BOOK=DICTIONARY&VA=OVARY",
                "NCI_DICTIONARY,_COMPREHENSIVE_CANCER_CENTER:HTTP://WWW.JAMESLINE.COM/CANCERTYPES/GLOSSARY/INDEX.CFM",
                "CHANNEL_CATFISH_FARMING_HANDBOOK:ISBN_0412123312"
            };
            for (String x : xrefs) {
                String u = x.toUpperCase();
                boolean found = false;
                for (String y : BOGUS) {
                    if (u.equalsIgnoreCase(y)) {
                        others.add(x);
                        found = true;
                        break;
                    }
                }
                if (!found)
                    useful.add(x);
            }
        }
        else if (ontology.resource != null
                 && "mp.owl".equals(ontology.resource.getLocalName())) {
            // i hate you!
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.equals("AAO:CURATOR")
                    || u.equals("AAO:DSM")
                    || u.startsWith("BAMS:")
                    || u.startsWith("GOC:")
                    || u.equals("GO:CURATOR")
                    || u.equals("GO:CVS")
                    || u.equals("GO:GO")
                    || u.equals("GO:DPH")
                    || u.equals("GO:KMV")
                    || u.equals("GO:TFM")
                    || u.startsWith("HPO:")
                    || u.startsWith("HTTP://")
                    || u.startsWith("HTTPS://")
                    || u.startsWith("ISBN:")
                    || u.startsWith("NCBI")
                    || u.startsWith("PATOC")
                    || u.startsWith("PHENOSCAPE:")
                    || u.startsWith("PMCID:")
                    || u.startsWith("PMID:")
                    || u.startsWith("ANSWERS.COM")
                    || u.startsWith("BIOLOGY-ONLINE:")
                    || u.startsWith("BOOK:")
                    || u.equals("ZFA:CURATOR")
                    || u.equals("CBN")
                    || u.equals("CHEBI")
                    || u.equals("CHEMBL")
                    || u.equals("CHEMIDPLUS")
                    || u.equals("IUPHAR")
                    || u.equals("CL:CVS")
                    || u.equals("CL:TM")
                    || u.equals("CL:MAH")
                    || u.equals("COME")
                    || u.startsWith("DICTIONARY:")
                    || u.equals("DRUGBANK")
                    || u.startsWith("DOI:")
                    || u.startsWith("FBC:")
                    || u.equals("FB:MA")
                    || u.equals("FEED:FEED")
                    || u.equals("FMA:TA")
                    || u.startsWith("GO_REF:")
                    || u.equals("HMDB")
                    || u.startsWith("IMPC:")
                    || u.startsWith("INFOVISUAL:")
                    || u.startsWith("IUPAC")
                    || u.equals("JCBN")
                    || u.equals("JB:JB")
                    || u.equals("KEGG_COMPOUND")
                    || u.equals("KEGG COMPOUND")
                    || u.equals("KEGG_DRUG")
                    || u.equals("DRUGCENTRAL")
                    || u.equals("PATO:GVG")
                    || u.equals("EUROPE PMC")
                    || u.equals("BEILSTEIN")
                    || u.equals("GMELIN")
                    || u.equals("REAXYS")
                    || u.equals("FMA:FMA")
                    || u.equals("AEO:JB")
                    || u.equals("SUBMITTER")
                    || u.equals("MA:TH") || u.equals("MA:MA")
                    || u.startsWith("MERRIAM-WEBSTER:")
                    || (u.startsWith("MGI:") && !Character.isDigit(u.charAt(4)))
                    || u.equals("MIG:ANNA") || u.startsWith("MITRE:")
                    || u.equals("MOLBASE") || u.startsWith("MONDOFACTO:")
                    || u.equals("MP:ANNA") || u.equals("MPATH:CURATION")
                    || u.startsWith("MPD:") || u.equals("MP:MP")
                    || u.startsWith("NIFSTD:")
                    || u.equals("NIST_CHEMISTRY_WEBBOOK")
                    || u.equals("NIST CHEMISTRY WEBBOOK")
                    || u.equals("LIPID MAPS") || u.equals("LIPID_MAPS")
                    || u.equals("AAO:LAP") || u.equals("AAO:EJS")
                    || u.startsWith("NPX:") || u.startsWith("OBOL:")
                    || u.startsWith("ORCID") || u.startsWith("PATHBASE:")
                    || u.startsWith("OXFORD:") || u.equals("PDBECHEM")
                    || u.startsWith("PRO:") || u.startsWith("RGD:")
                    || u.startsWith("TAO:") || u.startsWith("TAIR:")
                    || u.equals("SUBMITTER") || u.startsWith("SANBI:")
                    || u.startsWith("THEFREEDICTIONARY.COM")
                    || u.equals("UBERON:CJM") || u.equals("UM-BBD")
                    || u.equals("UNIPROT") || u.startsWith("VSAO:")
                    || u.equals("WHO_MEDNET") || u.startsWith("WORDNET:")
                    || u.startsWith("WTSI:") || u.startsWith("ZFIN:")
                    ) {
                    others.add(x);
                }
                else {
                    useful.add(x);
                }
            }
            
            Object label = data.get("label");
            if (label != null) {
                obj = Util.delta(label, new String[]{
                        "Europe PMC", "ChemIDplus", "Reaxys",
                        "KEGG COMPOUND", "DrugCentral", "Beilstein",
                        "KEGG DRUG", "ChEMBL", "Gmelin",
                        "NIST Chemistry WebBook", "LIPID MAPS",
                        "ChEMBL", "ChEBI", "SUBMITTER", "DrugBank",
                        "UM-BBD"
                    });
                if (obj != Util.NO_CHANGE)
                    data.put("label", obj);
            }
        }        
        else if (ontology.links.containsKey("versionIRI")
                 && ontology.links.get("versionIRI")
                 .toString().endsWith("efo.owl")) {

            Object smiles = data.remove("SMILES");
            if (smiles != null) {
                data.put("smiles", ((String)smiles).replaceAll
                         (Matcher.quoteReplacement("\\\\"),
                          Matcher.quoteReplacement("\\")));
            }
            
            for (String x : xrefs) {
                String u = x.toUpperCase();
                switch (u) {
                case "CHEBI":
                case "IUPAC":
                case "UNIPROT":
                case "KEGG_COMPOUND":
                case "ZFIN:CURATOR":
                case "OBOL:AUTOMATIC":
                case "NCIT:P378":
                case "FMA:TA":
                    others.add(x);
                break;
                default:
                    {
                        int start = x.indexOf(':'),
                            end = x.indexOf('\"', start);
                        if (end < 0) end = x.length();
                        if (u.startsWith("GOC:")
                            || u.startsWith("HPO:")) {
                            others.add(x);
                        }
                        else if (x.startsWith("MSH:")) {
                            useful.add("MESH:"+x.substring(start+1, end));
                        }
                        else if (x.startsWith("MONDO:")) {
                            if (Character.isDigit(x.charAt(start+1)))
                                useful.add(x);
                            else if (x.startsWith("patterns/", start+1)) {
                                String p = x.substring
                                    (x.indexOf('/', start+1)+1);
                                Object old = data.get("inSubset");
                                data.put("inSubset", old != null
                                         ? Util.merge(old, p) : p);
                                others.add(x);
                            }
                            else
                                others.add(x);
                        }
                        else if (end < x.length())
                            useful.add(x.substring(0, end).trim());
                    }
                }
            }
        }
        else if (ontology.links.containsKey("versionIRI")
                 && ontology.links.get("versionIRI")
                 .toString().endsWith("pato.owl")) {
            for (String x : xrefs) {
                if ("-".equals(x))
                    ;
                else if (x.startsWith("PATOC:") || x.startsWith("PATO:"))
                    others.add(x);
                else
                    useful.add(x);
            }
        }
        else if ("BrendaTissueOBO".equals
                 (ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                if (x.startsWith("http://purl.obolibrary.org/obo/BTO"))
                    useful.add(x);
                else
                    others.add(x);
            }
        }
        else if ("gene_ontology".equals
                 (ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.startsWith("GOC:")
                    || u.startsWith("PMID")
                    || u.startsWith("ISBN")
                    || u.startsWith("HTTP")
                    || u.startsWith("GO_REF")
                    || u.startsWith("WIKIPEDIA")
                    || (u.startsWith("GO:")
                        && !Character.isDigit(u.charAt(3)))
                    )
                    others.add(x);
                else
                    useful.add(x);
            }
        }
        else if ("chebi_ontology".equals
                 (ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.equals("KEGG_COMPOUND")
                    || u.equals("DRUGCENTRAL")
                    || u.equals("IUPAC")
                    || u.equals("CHEMIDPLUS")
                    || u.equals("KEGG_DRUG")
                    || u.equals("CHEBI")
                    || u.equals("WHO_MEDNET")
                    || u.equals("CHEMBL")
                    || u.equals("PDBECHEM")
                    || u.equals("NIST_CHEMISTRY_WEBBOOK")
                    || u.equals("DRUGBANK")
                    || u.equals("UNIPROT")
                    || u.equals("LIPID_MAPS")
                    || u.equals("METACYC")
                    || u.equals("HMDB")
                    || u.equals("SUBMITTER")
                    || u.equals("JCBN")
                    || u.equals("LECITHIN")
                    || u.equals("PHOSPHATIDYLCHOLINE")
                    || u.equals("SPHINGOMYELIN")
                    || u.equals("KEGG_GLYCAN")
                    || u.equals("MOLBASE")
                    || u.equals("ALAN_WOOD'S_PESTICIDES")
                    || u.equals("UM-BBD")
                    || u.equals("CBN")
                    || u.equals("SMID")
                    || u.equals("IUBMB")
                    || u.equals("KNAPSACK")
                    || u.equals("PATENT")
                    || u.equals("IUPHAR")
                    || u.equals("RESID")
                    || u.equals("COME")
                    || u.equals("PDB")
                    || u.equals("LINCS")
                    || u.equals("EMBL")
                    ) {
                    others.add(x);
                    Object old = data.get("inSubset");
                    data.put("inSubset",
                             old != null ? Util.merge(old, x) : x);
                }
                else if (u.startsWith("WIKIPEDIA:")
                         || u.startsWith("METACYC:")) {
                    int pos = x.indexOf(':');
                    String n = x.substring(pos+1).replaceAll("_", " ");
                    if (n.equalsIgnoreCase("LECITHIN")
                        || n.equalsIgnoreCase("PHOSPHATIDYLCHOLINE")
                        || n.equalsIgnoreCase("SPHINGOMYELIN")
                        || n.equalsIgnoreCase("Phosphatidylinositols")
                        || n.equalsIgnoreCase("TRIACYLGLYCEROLS")
                        ) {
                        Object old = data.get("inSubset");
                        data.put("inSubset",
                                 old != null ? Util.merge(old, n) :n);
                    }
                    else {
                        Object old = data.get("hasExactSynonym");
                        data.put("hasExactSynonym",
                                 old != null ? Util.merge(old, n) : n);
                        old = data.get("inSubset");
                        data.put("inSubset",
                                 old != null
                                 ? Util.merge(old, x.substring(0, pos))
                                 : x.substring(0, pos));
                    }
                }
                else if (u.startsWith("PMID:")) {
                    others.add(x);
                }
                else {
                    if (x.startsWith("CAS:")) {
                        data.put("CAS", x.substring(4));
                    }
                    useful.add(x);
                }
            }
                
            Object label = data.get("label");
            if (label != null) {
                obj = Util.delta(label, new String[]{
                        "Europe PMC", "ChemIDplus", "Reaxys",
                        "KEGG COMPOUND", "DrugCentral", "Beilstein",
                        "KEGG DRUG", "ChEMBL", "Gmelin",
                        "NIST Chemistry WebBook", "LIPID MAPS",
                        "ChEMBL", "ChEBI", "HMDB", "KNApSAcK",
                        "SUBMITTER", "MetaCyc", "DrugBank", "UM-BBD",
                        "Alan Wood's Pesticides", "Diglyceride",
                        "diglycerides", "Diacylglycerol", "SMID",
                        "Triacylglycerol", "triglyceride", "diglyceride",
                        "triglyceride", "Diglyceride"
                    });
                if (obj != Util.NO_CHANGE)
                    data.put("label", obj);
            }
                
            Object syn = data.get("hasRelatedSynonym");
            if (syn != null) {
                syn = filter (syn, p -> {
                        String s = (String)p;
                        return !(s.equalsIgnoreCase("Lecithin")
                                 || s.equalsIgnoreCase("Triacylglycerol")
                                 || s.equalsIgnoreCase("Diacylglycerol"));
                    });
                if (syn != null)
                    data.put("hasRelatedSynonym", syn);
            }
            
            Object old = data.get("inSubset");
            if (old != null) {
                Object dif = Util.delta(old, "_STAR");
                if (dif != Util.NO_CHANGE)
                    data.put("inSubset", dif);
            }
        }
        else if ("uberon".equals
                 (ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.equals("FMA:TA")
                    || u.equals("ZFIN:CURATOR")
                    || u.equals("ZFA:CURATOR")
                    || u.equals("TAO:WD")
                    || u.equals("MP:MP")
                    || u.equals("CL:TM")
                    || u.equals("MA:TH")
                    || u.equals("NIFSTD:NEURONAMES_ABBREVSOURCE")
                    || u.startsWith("NCBITAXON")
                    || u.startsWith("ISBN")
                    || u.startsWith("FEED")
                    || u.startsWith("GOC:")
                    || u.startsWith("FBC:")
                    || (u.startsWith("AAO:")
                        && !Character.isDigit(u.charAt(4)))
                    || (u.startsWith("UBERON:")
                        && !Character.isDigit(u.charAt(7)))
                    || u.startsWith("PHENOSCAPE")
                    || u.startsWith("MGI:")
                    || u.startsWith("BGEE")
                    || u.startsWith("XB:")
                    || u.startsWith("OBOL:")
                    || u.startsWith("HTTP")
                    || u.startsWith("DORLANDS")
                    )
                    others.add(x);
                else
                    useful.add(x);
            }
        }
        else if ("disease_ontology".equals
                 (ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                if (u.equals("MTH:NOCODE")
                    || u.equals("LS:IEDB")
                    || u.equals("SN:IEDB")
                    || u.startsWith("URL")
                    || u.startsWith("DO:")
                    || u.startsWith("MTHICD9")
                    || u.startsWith("JA:")
                    || u.startsWith("HTTP")
                    )
                    others.add(x);
                else
                    useful.add(x);
            }
        }
        else if ("human_phenotype".equals
                 (ontology.props.get("default-namespace"))) {
            // for hpo, hasDbXref in axiom corresponds to curator or 
            // publication. don't stitch on these
            for (String x : xrefs) {
                String u = x.toUpperCase();
                // sigh.. non-informative xrefs; why are these xrefs?
                if (u.startsWith("UMLS")
                    || (u.startsWith("HP:")
                        && !u.equals("HP:PROBINSON"))
                    || u.startsWith("SNOMEDCT")
                    || (u.startsWith("FMA:")
                        && Character.isDigit(u.charAt(4)))
                    || (u.startsWith("AAO:")
                        && Character.isDigit(u.charAt(4)))
                    || (u.startsWith("BTO:")
                        && Character.isDigit(u.charAt(4)))
                    ) {
                    useful.add(x);
                }
                else if (u.charAt(0) == 'C'
                         && Character.isDigit(u.charAt(1))) {
                    useful.add("UMLS:"+u);
                }
                else if (u.startsWith("MSH")) {
                    useful.add(x.replaceAll("MSH", "MESH"));
                }
                else
                    others.add(x);
            }
        }
        else if ("protein".equals(ontology.props.get("default-namespace"))) {
            for (String x : xrefs) {
                if (x.startsWith("PRO:")) {
                    others.add(x);
                }
                else {
                    useful.add(x);
                }
            }
            
            String comment = (String) data.get("comment");
            if (comment != null) {
                if (comment.startsWith("Category=gene")) {
                    // human protein
                    Set<String> genes = new TreeSet<> ();
                    obj = data.get("hasExactSynonym");
                    if (obj != null) {
                        Object[] values = Util.toArray(obj);
                        for (Object v : values) {
                            String s = v.toString();
                            if (s.indexOf(' ') > 0) {
                            }
                            else {
                                genes.add(s);
                            }
                        }
                    }
                    
                    obj = data.get("hasRelatedSynonym");
                    if (obj != null) {
                        Object[] values = Util.toArray(obj);
                        for (Object v : values) {
                            String s = v.toString();
                            if (s.indexOf(' ') > 0) {
                            }
                            else {
                                genes.add(s);
                            }
                        }
                    }
                    
                    if (!genes.isEmpty()) {
                        //logger.info("** GENES: "+genes);
                        if (genes.size() == 1) {
                            data.put("GENESYMBOL", genes.iterator().next());
                        }
                        else {
                            data.put("GENESYMBOL",
                                     genes.toArray(new String[0]));
                        }
                    }
                }
                else if (comment.startsWith("Category=organism-gene")) {
                }
            }
        }
        else if (ontology.props.get("title") != null
                 && "mondo.owl".equals(ontology.resource.getLocalName())) {
            for (String x : xrefs) {
                String u = x.toUpperCase();
                // these are not stitch identifiers
                if (u.startsWith("MONDO")
                    || u.equals("NCIT:P378")
                    || u.equals("MTH:NOCODE")
                    || u.startsWith("DOI:")
                    || u.startsWith("URL")
                    || u.startsWith("HTTP")
                    || u.startsWith("PMID")
                    || u.startsWith("WIKIPEDIA")
                    ) {
                    others.add(x);
                }
                else
                    useful.add(x);
            }
        }
        else if (ontology.props.get("title") != null
                 && ((String)ontology.props.get("title")).startsWith("OGG")) {
            for (String x : xrefs) {
                if (x.startsWith("MIM:"))
                    useful.add("O"+x);
                else
                    useful.add(x);
            }
            
            obj = data.get("OGG_0000000006");
            if (obj != null)
                useful.add("GENE:"+obj);
            obj = data.get("OGG_0000000030");
            if (obj != null) {
                // pmid
                List<Long> pmids = new ArrayList<>();
                for (String tok : obj.toString().split("\\s")) {
                    try {
                        long pmid = Long.parseLong(tok);
                        pmids.add(pmid);
                    }
                    catch (NumberFormatException ex) {
                    }
                }
                
                if (!pmids.isEmpty()) {
                    // override
                    data.put("OGG_0000000030", pmids.toArray(new Long[0]));
                }
            }

            /*
            obj = data.get("OGG_0000000029");
            if (obj != null) {
                List<String> annotations = new ArrayList<>();
                for (String tok : obj.toString().split(";")) {
                    String t = tok.trim();
                    if (t.startsWith("GO_")) {
                        int pos = t.indexOf(' ');
                        if (pos > 0)
                            t = t.substring(0, pos);
                        annotations.add("GO:"+t.substring(3));
                    }
                }
                
                if (!annotations.isEmpty()) {
                    data.put("has_go_association",
                             annotations.toArray(new String[0]));
                }
            }
            */
        }
        else if ("MEDLINEPLUS".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null)
                data.put("notation", map (obj, a -> "UMLS:"+a));
        }
        else if ("OMIM".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null) {
                data.put("notation", map (obj, a -> {
                            if (Character.isDigit(((String)a).charAt(0)))
                                return "OMIM:"+a;
                            return a;
                        }));
            }
            
            obj = data.get("altLabel");
            if (obj != null) {
                obj = Util.delta(obj, new String[]{
                        "VARIANT OF UNKNOWN SIGNIFICANCE",
                        "RECLASSIFIED - VARIANT OF UNKNOWN SIGNIFICANCE"
                    });
                if (obj != Util.NO_CHANGE)
                    data.put("altLabel", obj);
            }
        }
        else if ("MSH".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null)
                data.put("notation", map (obj, a -> "MESH:"+a));
        }
        else if ("RXNORM".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null)
                data.put("notation", map (obj, a -> "RXNORM:"+a));
        }
        else if ("VANDF".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null)
                data.put("notation", map (obj, a -> "VANDF:"+a));
        }
        else if ("ICD10CM".equals(ontology.props.get("label"))) {
            obj = data.remove("notation");
            if (obj != null)
                data.put("notation", map (obj, a -> "ICD10CM:"+a));
        }
        
        if (!useful.isEmpty() || !others.isEmpty()) {
            xrefs = useful;
            if (!others.isEmpty())
                data.put("_hasDbXref", others.toArray(new String[0]));
        }
        
        if (!xrefs.isEmpty())
            data.put("hasDbXref", xrefs.toArray(new String[0]));
            
        if (!icds.isEmpty())
            data.put("ICD", icds.toArray(new String[0]));

        //logger.info("... registering: "+data);
        return data;
    } // sigh
    
    protected Entity _registerIfAbsent (OntologyResource or) {
        Map<String, Object> data = new TreeMap<>();
        data.put(Props.URI, or.uri);
        data.putAll(or.props);
        if (!data.containsKey("notation")) {
            String not = or.resource.getLocalName();
            if (not != null)
                data.put("notation", not.replaceAll("_", ":"));
        }

        for (Map.Entry<String, Object> me : or.links.entrySet()) {
            if (isDeferred (me.getKey()))
                continue;
            
            Object value = me.getValue();
            List<Resource> links = new ArrayList<>();
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                Object vs = null;
                for (int i = 0; i < len; ++i) {
                    Resource r = (Resource) Array.get(value, i);
                    vs = Util.merge(vs, getResourceValue (r));
                    links.add(r);
                }
                data.put(me.getKey(), vs);
            }
            else {
                Resource r = (Resource) value;
                data.put(me.getKey(), getResourceValue (r));
                links.add(r);
            }
            
            for (Resource r : links) {
                String rv = r.getLocalName();
                Object old = data.get(me.getKey());
                data.put(me.getKey(), old != null ? Util.merge(old, rv) : rv);
            }
        }

        // now process all axioms that aren't deferred
        for (OntologyResource ax : or.axioms) {
            Resource r = (Resource) ax.links.get("annotatedProperty");
            String rn = r.getLocalName();
            if (!isDeferred (rn)) {
                Resource t = (Resource) ax.links.get("annotatedTarget");
                Object old = data.get(rn);
                if (t != null) {
                    data.put(rn, old != null
                             ? Util.merge(old, t.getLocalName())
                             : t.getLocalName());
                }
                else {
                    Object v = ax.props.get("annotatedTarget");
                    data.put(rn, old != null ? Util.merge(old, v) : v);
                }
            }

            // copy non-annotated properties
            for (Map.Entry<String, Object> me : ax.props.entrySet()) {
                if (!me.getKey().startsWith("annotated")) {
                    Object old = data.get(me.getKey());
                    data.put(me.getKey(), old != null
                             ? Util.merge(old, me.getValue()) : me.getValue());
                }
            }
        }

        Entity ent = register (sigh (data));
        if (or.props.isEmpty() && or.axioms.isEmpty()) {
            // transient entity
            ent.addLabel(AuxNodeType.TRANSIENT);
        }
        
        Object deprecated = data.get("deprecated");
        if (deprecated != null
            && "true".equalsIgnoreCase(deprecated.toString())) {
            ent.set(Props.STATUS, "deprecated");
        }
        
        return ent;
    }

    void _stitch (Entity ent, String name, OntologyResource or) {
        if (or.isRestriction()) {
            Resource res = (Resource) or.links.get("onProperty");
            String prop = getURI (res); //getResourceValue (res);

            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put(Props.NAME, name);
            attrs.put(Props.PROPERTY, prop);
            attrs.put(Props.SOURCE, source.getKey());

            String val = (String)or.props.get("hasValue");
            if (val != null) {
                //logger.info("####Restriction: property="+res+" value="+val);
                //ent._payload(prop, val);
                //attr.put(Props.VALUE, val);
            }
            else {
                for (Map.Entry<String, Object> me : or.links.entrySet()) {
                    switch (me.getKey()) {
                    case "someValuesFrom":
                    case "allValuesFrom":
                    case "onDataRange":
                        {
                            res = (Resource) me.getValue();
                            prop = getURI (res);
                            if (prop != null) {
                                val = prop;
                            }
                            else {
                                _stitch (ent, name, res, attrs);
                                return;
                            }
                        }
                    }
                }
            }
                
            StitchKey key = R_rel;
            switch (name) {
            case "subClassOf":
                key = R_subClassOf;
                break;
            }

            if (val != null) {
                for (Iterator<Entity> iter = find (Props.URI, prop);
                     iter.hasNext();) {
                    Entity e = iter.next();
                    if (!ent.equals(e)) 
                        ent._stitch(e, key, val, attrs);
                }
            }
            else {
                logger.warning("***** UNKNOWN RESOURCE...\n"+or);
            }
        }
    }

    void _stitch (Entity ent, String name, Resource res) {
        _stitch (ent, name, res, null);
    }
    
    void _stitch (Entity ent, String name,
                  Resource res, Map<String, Object> attrs) {
        String uri = getURI (res);
        if (DEBUG > 0) {
            logger.info("+++++ stitching resource "+res+" (uri="+uri
                        +") to entity "+ent.getId()+" via "+name+"...");
        }
        
        if (uri != null || resources.containsKey(res)) {
            if (uri == null) {
                uri = res.toString(); // anonymous class
            }

            if (attrs == null) {
                attrs = new LinkedHashMap<>();
                attrs.put(Props.NAME, name);
                attrs.put(Props.SOURCE, source.getKey());
            }
            
            for (Iterator<Entity> iter = find (Props.URI, uri);
                 iter.hasNext();) {
                Entity e = iter.next();
                if (!e.equals(ent)) {
                    switch (name) {
                    case "subClassOf":
                        ent._stitch(e, R_subClassOf, uri, attrs);
                        break;
                    case "equivalentClass":
                        ent._stitch(e, R_equivalentClass, uri, attrs);
                        break;
                    case "exactMatch":
                        ent._stitch(e, R_exactMatch, uri, attrs);
                        break;
                    case "closeMatch":
                        ent._stitch(e, R_closeMatch, uri, attrs);
                        break;
                    default:
                        ent._stitch(e, R_rel, uri, attrs);
                        //logger.warning("Unknown stitch relationship: "+name);
                    }
                }
            }
        }
        else if (xrefs.containsKey(res)) {
            OntologyResource or = xrefs.get(res);
            if (DEBUG > 0) {
                logger.info("~~~~~ resolving "+res+" to entity "
                            +ent.getId()+" via "+name+"\n"+or);
            }
            
            for (Map.Entry<String, Object> me : or.links.entrySet()) {
                if (DEBUG > 0) {
                    logger.info("..."+me.getKey()+": "+me.getValue().getClass()
                                +" "+me.getValue());
                }
                
                Resource r = (Resource)me.getValue();
                if (xrefs.containsKey(r)) {
                    OntologyResource ores = xrefs.get(r);
                    _stitch (ent, name, ores.resource);
                }
                else {
                    OntologyResource ores = new OntologyResource (r);
                    if (DEBUG > 0) {
                        logger.info("^^^^^"+ores);
                    }
                    _stitch (ent, name, ores);
                }
            }
        }
        else {
            _stitch (ent, name, new OntologyResource (res));
        }
    }

    protected void _resolve (OntologyResource or) {
        List<Entity> entities = new ArrayList<>();
        if (or.uri != null) {
            for (Iterator<Entity> iter = find (Props.URI, or.uri);
                 iter.hasNext();) {
                Entity e = iter.next();
                entities.add(e);
            }
        }
        
        if (!entities.isEmpty()) {
            if (DEBUG > 0) {
                logger.info
                    ("~~~~ "+or.uri+" => "+entities.size()+" "+or.links.keySet());
            }
            
            for (Map.Entry<String, Object> me : or.links.entrySet()) {
                if (RELATIONS.contains(me.getKey())) {
                    Object value = me.getValue();
                    List<Resource> links = new ArrayList<>();
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        for (int i = 0; i < len; ++i) {
                            Resource res = (Resource) Array.get(value, i);
                            links.add(res);
                        }
                    }
                    else {
                        Resource res = (Resource) value;
                        links.add(res);
                    }

                    for (Entity e : entities) {
                        for (Resource res : links) {
                            _stitch (e, me.getKey(), res);
                        }
                    }
                }
                /*
                  else if (getStitchKey (me.getKey()) == null) {
                  logger.warning("Unknown link resource: "+me.getKey());
                  }
                */
            }
            
            // now axioms
            for (OntologyResource ox : or.axioms) {
                Resource r = (Resource)ox.links.get("annotatedProperty");
                String rn = r.getLocalName();
                if (isDeferred (rn)) {
                    r = (Resource)ox.links.get("annotatedTarget");
                    if (r != null) {
                        for (Entity e : entities)
                            _stitch (e, rn, r, ox.props);
                    }
                }
            }
        }
        else {
            //logger.warning("Unable to resolve "+or.uri);
            Entity ent = _registerIfAbsent (or);
            logger.info("+++++++ "+ent.getId()+" +++++++\n"+or);
        }
    }

    protected void resolve (OntologyResource or) {
        try (Transaction tx = gdb.beginTx()) {
            _resolve (or);
            tx.success();
        }
    }
    
    protected Entity registerIfAbsent (OntologyResource or) {
        Entity ent = null;
        try (Transaction tx = gdb.beginTx()) {
            ent = _registerIfAbsent (or);
            tx.success();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't register resource: "+or, ex);
            throw new RuntimeException (ex);
        }
        return ent;
    }

    public DataSource register (String file) throws Exception {
        DataSource ds = super.register(new File (file));

        Model model = ModelFactory.createDefaultModel();
        model.read(file);
        reset ();

        ResIterator iter = model.listSubjects();
        while (iter.hasNext()) {
            Resource res = iter.nextResource();
            OntologyResource or = new OntologyResource (res);
            if (or.isOntology()) {
                ontology = or;
                for (Map.Entry<String, Object> me : or.props.entrySet())
                    if (!"".equals(me.getKey()))
                        ds.set(me.getKey(), me.getValue());
                
                for (Map.Entry<String, Object> me : or.links.entrySet()) {
                    Object value = me.getValue();
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        String[] vals = new String[len];
                        for (int i = 0; i < len; ++i) {
                            Resource r = (Resource) Array.get(value, i);
                            vals[i] = getURI (r);
                        }
                        ds.set(me.getKey(), vals);
                    }
                    else {
                        Resource r = (Resource) me.getValue();
                        String uri = getURI (r);
                        if (uri != null)
                            ds.set(me.getKey(), uri);
                    }
                }
                logger.info(">>>>>>> Ontology <<<<<<<<\n"+or);
                break;
            }
        }
        iter.close();

        if (ontology == null) {
            logger.warning("!!! No ontology class found! !!!");
        }
        
        Set<OntologyResource> axioms = new HashSet<>();
        
        logger.info("Loading resources...");
        model = ModelFactory.createDefaultModel();
        model.read(file);
        iter = model.listSubjects();
        while (iter.hasNext()) {
            Resource res = iter.nextResource();
            OntologyResource or = new OntologyResource (res);
            if (or.isOntology()) {
            }
            else if (or.isAxiom()) {
                res = (Resource) or.links.get("annotatedSource");
                OntologyResource ref = resources.get(res);
                if (ref != null)
                    ref.axioms.add(or);
                else
                    axioms.add(or);
            }
            else if (or.isRestriction()) {
                //logger.warning("############# Restriction:\n"+or);
                //resources.put(res, or);
            }
            else if (or.isClass() || "ObjectProperty".equals(or.type)
                     || "AnnotationProperty".equals(or.type)) {
                OntologyResource old = resources.put(res, or);
                if (old != null) {
                    logger.warning
                        ("Class "+res+" already mapped to\n"+old);
                }
            }
            else {
                /*
                logger.warning("Resource type "
                               +or.type+" not recognized:\n"+or);
                */
                xrefs.put(res, or);
            }

            if (false
                && ("http://www.orpha.net/ORDO/Orphanet_1000".equals(or.uri)
                    || resources.size() > 2000)
                ) {
                //System.err.println(or);
                break;
            }
        }
        iter.close();
        logger.info("###### "+resources.size()+" class resources and "
                    +axioms.size()+" axioms parsed!");

        List<OntologyResource> unresolved = new ArrayList<>();
        for (OntologyResource or : axioms) {
            Resource res = (Resource) or.links.get("annotatedSource");
            OntologyResource ref = resources.get(res);
            if (ref != null) {
                ref.axioms.add(or);
            }
            else {
                //logger.warning("Unable to resolve axiom:\n"+or);
                unresolved.add(or);
            }
        }

        /*
        for (Map.Entry<Resource, OntologyResource> me : resources.entrySet()) {
            logger.info("-- "+me.getKey()+"\n"+me.getValue());
        }
        */
        
        // register entities
        logger.info("####### registering entities...");
        for (Map.Entry<Resource, OntologyResource> me : resources.entrySet()) {
            Entity ent = registerIfAbsent (me.getValue());
            logger.info("+++++++ "+ent.getId()+" +++++++\n"
                        +me.getKey()+"\n"+me.getValue());
        }

        // resolve other references (if any)
        logger.info("####### resolving other entities...");
        for (OntologyResource or : xrefs.values()) {
            if (or.uri != null)
                resolve (or);
        }
        
        // resolve entities
        logger.info("####### resolving class entities...");
        for (OntologyResource or : resources.values()) {
            resolve (or);
        }
        
        if (!unresolved.isEmpty()) {
            logger.warning("!!!!! "+unresolved.size()
                           +" unresolved axioms !!!!!");
            for (OntologyResource or : unresolved)
                System.out.println(or);
        }

        ds.set(INSTANCES, resources.size());
        updateMeta (ds);

        return ds;
    }

    static void iterateModel (Model model) {
        Map<String, Integer> classes = new TreeMap<>();
        for (ResIterator iter = model.listSubjects(); iter.hasNext();) {
            Resource res = iter.next();
            String name = res.getLocalName();
            if (name != null && name.startsWith("MONDO")) {
                Integer c = classes.get(name);
                classes.put(name,c==null?1:c+1);
            }
            
            System.out.println(name+" "+res);
            for (StmtIterator it = res.listProperties(); it.hasNext(); ) {
                Statement stm = it.next();
                Property prop = stm.getPredicate();
                RDFNode obj = stm.getObject();
                System.out.print("-- "+prop.getLocalName()+": ");
                if (obj.isResource()) {
                    System.out.print(getURI (obj.asResource()));
                }
                else if (obj.isLiteral()) {
                    System.out.print(obj.asLiteral().getValue());
                }
                System.out.println();
            }
            System.out.println();
        }
        System.out.println("OWL classes: "+classes.size());
    }

    static void iterateModel (OntModel model) {
        model.setDynamicImports(false);
        model.loadImports();
        
        System.out.println("Import Ontologies:");
        for (String uri : model.listImportedOntologyURIs()) {
            System.out.println("-- "+uri);
            model.addLoadedImport(uri);
        }

        System.out.println(model.countSubModels()+" sub models!");
        
        System.out.println("Annotation Properties:");
        for (ExtendedIterator<AnnotationProperty> iter =
                 model.listAnnotationProperties(); iter.hasNext(); ) {
            AnnotationProperty prop = iter.next();
            System.out.println("-- "+prop.getLocalName());
        }

        System.out.println("Classes:");
        int nc = 0;
        for (ExtendedIterator<OntClass> iter =
                 //model.listHierarchyRootClasses();
                 model.listNamedClasses();
             iter.hasNext(); ) {
            OntClass ont = iter.next();
            System.out.println("-- "+ont.getLabel(null)+" "+ont);
            ++nc;
        }
        System.out.println(nc+" classes!");
    }
    
    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+OntEntityFactory.class.getName()
                        +" DBDIR [cache=DIR] [OWL|TTL]...");
            System.exit(1);
        }

        try (OntEntityFactory def = new OntEntityFactory (argv[0])) {
            int i = 1;
            if (argv[i].startsWith("cache=")) {
                def.setCache(argv[i].substring(6));
                ++i;
            }

            for (; i < argv.length; ++i)
                def.register(argv[i]);
        }
        
        /*
        // why is processing ontology model so slow?
        //OntModel model = ModelFactory.createOntologyModel();
        Model model = ModelFactory.createDefaultModel();
        // read the RDF/XML file
        model.read(argv[0]);

        //Model model = RDFDataMgr.loadModel(argv[0]);
        logger.info(argv[0]+": "+model.getClass());
        iterateModel (model);
        
        // write it to standard out
        //model.write(System.out);         
        */
    }
}
