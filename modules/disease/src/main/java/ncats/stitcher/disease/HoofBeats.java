package ncats.stitcher.disease;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.JsonNode;

public class HoofBeats {
    static final Logger logger = Logger.getLogger(HoofBeats.class.getName());
    static final int BATCH_SIZE = 200;
    static final String RANCHO_LABEL = "S_RANCHO-DISEASE-DRUG_2018-12-18_13-30";
    static final SimpleDateFormat DF = new SimpleDateFormat ("MM/dd/yy");
    
    static class Source implements Comparable {
        final String name;
        final String[] types;
        final String id;
        final String prefix;
        final String label;
        final String source;
        final String[] synonyms;

        Source (String name, String id, String prefix,
                String label, String source, String... synonyms) {
            this (name, null, id, prefix, label, source, synonyms);
        }
        Source (String name, String[] types, String id, String prefix,
                String label, String source, String... synonyms) {
            this.name = name;
            this.types = types;
            this.id = id;
            this.prefix = prefix;
            this.label = label;
            this.source = source;
            this.synonyms = synonyms;
        }

        public String url (Entity e) {
            switch (name) {
            case "S_GARD":
                return "https://rarediseases.info.nih.gov/diseases/"
                    +e.payload("id")+"/index";
            case "S_MEDGEN":
                return "https://www.ncbi.nlm.nih.gov/medgen/"+e.payload("id");
            case "S_OMIM":
                { String id = getString (e.payload("notation"));
                    return "https://omim.org/entry/"+id.substring(id.indexOf(':')+1);
                }
            }
            return getString (e.payload("uri"));
        }

        public boolean valid (Entity e) {
            return e.is(name) && (types == null || e.hasAnyLabels(types));
        }
        
        public int compareTo (Object obj) {
            if (obj instanceof Source)
                return name.compareTo(((Source)obj).name);
            return name.compareTo(obj.toString());
        }
        public boolean equals (Object obj) {
            if (obj instanceof Source) {
                return name.equals(((Source)obj).name);
            }
            return name.equals(obj);
        }
        public int hashCode () { return name.hashCode(); }
        public String toString () { return name; }
    }
    
    static final Set<Source> SOURCES = new TreeSet<>();
    static {
        SOURCES.add(new Source ("S_MONDO", "notation", "", "label", "MONDO",
                                "hasExactSynonym", "hasRelatedSynonym"));
        SOURCES.add(new Source ("S_ORDO_ORPHANET", "notation", "", "label",
                                "Orphanet", "alternative_term"));
        SOURCES.add(new Source ("S_GARD", "gard_id", "", "name",
                                "GARD", "synonyms"));
        SOURCES.add(new Source ("S_OMIM", new String[]{
                    "T047", "T019", "T191", "T190", "T184"},
                "notation", "", "label", "OMIM", "altLabel"));
        SOURCES.add(new Source ("S_MEDGEN", new String[]{
                    "T047", "T191", "T019", "T190", "T184"},
                "id", "MEDGEN:", "NAME", "MedGen", "SYNONYMS"));
        SOURCES.add(new Source ("S_MESH", new String[]{
                    "T047", "T191", "T019", "T190", "T184"},
                "notation", "", "label", "MeSH", "altLabel"));
        SOURCES.add(new Source ("S_DOID", "notation", "", "label",
                                "DiseaseOntology", "hasExactSynonym"));
        SOURCES.add(new Source ("S_MEDLINEPLUS", "notation", "",
                                "label", "MedlinePlus", "altLabel"));
        SOURCES.add(new Source ("S_EFO", "notation", "", "label", "EFO",
                                "hasExactSynonym", "hasRelatedSynonym"));
        SOURCES.add(new Source ("S_ICD10CM", "notation", "", "label",
                                "ICD10", "altLabel"));
    }

    static final String HP_CATEGORY_FMT = "match (d:DATA)-->(n:S_HP)-"
        +"[e:R_subClassOf*0..13]->(m:S_HP)-[:R_subClassOf]->(:S_HP)--(z:DATA) "
        +"where d.notation='%1$s' and all(x in e where x.source=n.source or "
        +"n.source in x.source) and z.notation='HP:0000118' with m match "
        +"p=(m)<--(d:DATA) return distinct d.label as label order by label";

    static final String CATEGORY_FMT = "match (d:DATA)-->(n:`%1$s`)-"
        +"[e:R_subClassOf*0..13]->(m:`%1$s`)-[:R_subClassOf]->(:`%1$s`)"
        +"--(z:DATA) where d.notation='%2$s' and all(x in e where "
        +"x.source=n.source or n.source in x.source) and z.notation='%3$s' "
        +"with m match p=(m)<--(d:DATA) return distinct d.label as label, "
        +"d.notation as notation order by label";

    static final String MONDO_CATEGORY_FMT = "match (d:DATA)-->(n:`S_MONDO`)-[e:R_subClassOf*0..13]->(m:`S_MONDO`)<-[:PAYLOAD]-(z:DATA) where d.notation='%1$s' and all(x in e where x.source=n.source or n.source in x.source) and z.notation IN ["
        +"\"MONDO:0003900\","
        +"\"MONDO:0021147\","
        +"\"MONDO:0045024\","
        +"\"MONDO:0024297\","
        +"\"MONDO:0045028\","
        +"\"MONDO:0000839\","
        +"\"MONDO:0024236\","
        +"\"MONDO:0021178\","
        +"\"MONDO:0021166\","
        +"\"MONDO:0005550\","
        +"\"MONDO:0003847\","
        +"\"MONDO:0020683\","
        +"\"MONDO:0002254\","
        +"\"MONDO:0002025\","
        +"\"MONDO:0020012\","
        +"\"MONDO:0021669\","
        +"\"MONDO:0024575\","
        +"\"MONDO:0002409\","
        +"\"MONDO:0004995\","
        +"\"MONDO:0004335\","
        +"\"MONDO:0021145\","
        +"\"MONDO:0024458\","
        +"\"MONDO:0005151\","
        +"\"MONDO:0005570\","
        +"\"MONDO:0005046\","
        +"\"MONDO:0002051\","
        +"\"MONDO:0002081\","
        +"\"MONDO:0005071\","
        +"\"MONDO:0005087\","
        +"\"MONDO:0002118\","
        +"\"MONDO:0100120\","
        +"\"MONDO:0005135\","
        +"\"MONDO:0019751\","
        +"\"MONDO:0007179\","
        +"\"MONDO:0004992\","
        +"\"MONDO:0024674\","
        +"\"MONDO:0024882\","
        +"\"MONDO:0021058\","
        +"\"MONDO:0100120\","
        +"\"MONDO:0005135\","
        +"\"MONDO:0019052\","
        +"\"MONDO:0044970\","
        +"\"MONDO:0005365\","
        +"\"MONDO:0019512\","
        +"\"MONDO:0005267\","
        +"\"MONDO:0005385\","
        +"\"MONDO:0015111\","
        +"\"MONDO:0005020\","
        +"\"MONDO:0006858\","
        +"\"MONDO:0002356\","
        +"\"MONDO:0002332\","
        +"\"MONDO:0004298\","
        +"\"MONDO:0002263\","
        +"\"MONDO:0005047\","
        +"\"MONDO:0003150\","
        +"\"MONDO:0005328\","
        +"\"MONDO:0002135\","
        +"\"MONDO:0021084\","
        +"\"MONDO:0001941\","
        +"\"MONDO:0005495\","
        +"\"MONDO:0015514\","
        +"\"MONDO:0005154\","
        +"\"MONDO:0100070\","
        +"\"MONDO:0002356\","
        +"\"MONDO:0001223\","
        +"\"MONDO:0003393\","
        +"\"MONDO:0003240\","
        +"\"MONDO:0002280\","
        +"\"MONDO:0001531\","
        +"\"MONDO:0002245\","
        +"\"MONDO:0003804\","
        +"\"MONDO:0003225\","
        +"\"MONDO:0044348\","
        +"\"MONDO:0017769\","
        +"\"MONDO:0005271\","
        +"\"MONDO:0009453\","
        +"\"MONDO:0003778\","
        +"\"MONDO:0005093\","
        +"\"MONDO:0024255\","
        +"\"MONDO:0006607\","
        +"\"MONDO:0006615\","
        +"\"MONDO:0019296\","
        +"\"MONDO:0005218\","
        +"\"MONDO:0005172\","
        +"\"MONDO:0023603\","
        +"\"MONDO:0002602\","
        +"\"MONDO:0005560\","
        +"\"MONDO:0005027\","
        +"\"MONDO:0005559\","
        +"\"MONDO:0002320\","
        +"\"MONDO:0005287\","
        +"\"MONDO:0019117\","
        +"\"MONDO:0020010\","
        +"\"MONDO:0005395\","
        +"\"MONDO:0003620\","
        +"\"MONDO:0100081\","
        +"\"MONDO:0000270\","
        +"\"MONDO:0005275\","
        +"\"MONDO:0000376\","
        +"\"MONDO:0004867\","
        +"\"MONDO:0005240\","
        +"\"MONDO:0006026\","
        +"\"MONDO:0020059\","
        +"\"MONDO:0007254\","
        +"\"MONDO:0008170\","
        +"\"MONDO:0001657\","
        +"\"MONDO:0006517\","
        +"\"MONDO:0021063\","
        +"\"MONDO:0005575\","
        +"\"MONDO:0008903\","
        +"\"MONDO:0002120\","
        +"\"MONDO:0005206\","
        +"\"MONDO:0002898\","
        +"\"MONDO:0004950\","
        +"\"MONDO:0001627\","
        +"\"MONDO:0006999\","
        +"\"MONDO:0044872\","
        +"\"MONDO:0003441\","
        +"\"MONDO:0019956\","
        +"\"MONDO:0020640\","
        +"\"MONDO:0020067\","
        +"\"MONDO:0001835\","
        +"\"MONDO:0001071\","
        +"\"MONDO:0000437\","
        +"\"MONDO:0005258\","
        +"\"MONDO:0017713\","
        +"\"MONDO:0016054\","
        +"\"MONDO:0018075\","
        +"\"MONDO:0008449\","
        +"\"MONDO:0019716\","
        +"\"MONDO:0002561\","
        +"\"MONDO:0020121\"]"
        +"return distinct z.label as label, z.notation as notation order by label";

    static final String[] HPO_TYPE_IMAGING_MRI = {
        "HP:0002500", "HP:0030890", "HP:007103",
        "HP:0012696", "HP:0012747", "HP:0012751",
        "HP:0040332", "HP:0040329", "HP:0002419",
        "HP:0040272", "HP:0007183", "HP:0040331",
        "HP:0040328", "HP:0040333", "HP:0040330",
        "HP:0007266", "HP:0032615", "HP:0500016"
    };
        
    static final String HP_INHERITANCE_FMT = "match (d:DATA)-->(n:S_HP)-[e:R_subClassOf*0..]->(:S_HP)--(z:DATA) where d.notation='%1$s' and all(x in e where x.source=n.source or n.source in x.source) and z.notation='HP:0000005' return d.label as inheritance";

    // HP:0040006 - mortality/aging
    // HP:0003674 - onset

    static final String CATEGORY_NONE = "";
    
    // lookup of GR short id to book id
    static final Map<String, String> GENEREVIEWS = new HashMap<>();
    static {
        try (BufferedReader br = new BufferedReader
             (new FileReader ("GRshortname_NBKid_genesymbol_dzname.txt"))) {
            for (String line; (line = br.readLine()) != null; ) {
                String[] toks = line.split("\\|");
                GENEREVIEWS.put(toks[0], toks[1]);
            }
            logger.info(GENEREVIEWS.size()+" Gene Reviews LUT loaded!");
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

    static String getHpCategoryCypher (String id) {
        return getHpCategoryCypher (id, "HP:0000118");
    }
    
    static String getHpCategoryCypher (String id, String root) {
        return String.format(CATEGORY_FMT, "S_HP", id, root);
    }

    static String getMondoCategoryCypher (String id) {
        return getMondoCategoryCypher (id, "MONDO:0000001");
    }

    static String getMondoCategoryCypher (String id, String root) {
        return String.format(CATEGORY_FMT, "S_MONDO", id, root);
    }
    
    static Source getSource (String name) {
        for (Source s : SOURCES)
            if (s.equals(name))
                return s;
        //logger.log(Level.SEVERE, "BOGUS SOURCE: "+name);
        return null;
    }

    static String escape (String s) {
        StringBuilder sb = new StringBuilder ();
        s = s.trim();
        for (int i = 0; i < s.length(); ++i) {
            char ch = s.charAt(i);
            switch (ch) {
            case '\n':
                sb.append("\\n");
                break;
            case '\b':
                sb.append("\\b");
                break;
            case '\f':
                sb.append("\\f");
                break;
            case '\r':
                sb.append("\\r");
                break;
            case '\t':
                sb.append("\\t");
                break;
            case '"':
                sb.append("\\");
                // fall thru
            default:
                sb.append(ch);
            }
        }
        return sb.toString();
    }
    
    static String getString (Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
                StringBuilder sb = new StringBuilder ();
                for (int i = 0; i < Array.getLength(value); ++i) {
                    String s = (String)Array.get(value, i);
                    if (s.length() > 0) {
                        if (sb.length() > 0) sb.append("; ");
                        sb.append(s);
                    }
                }
                value = sb.toString();
            }
            return escape (value.toString());
        }
        return "";
    }
    
    final EntityFactory ef;    
    final UnionFind uf = new UnionFind ();
    final ObjectMapper mapper = new ObjectMapper ();
    final Map<String, String> hpCategories = new HashMap<>();

    class OrphanetGenes implements NeighborVisitor {
        ArrayNode genes;
        Set<Entity> neighbors = new LinkedHashSet<>();
        String notation;

        OrphanetGenes (Entity... ents) {
            this (mapper.createArrayNode(), ents);
        }
        OrphanetGenes (ArrayNode genes, Entity... ents) {
            this.genes = genes;
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    notation = getString (e.payload("notation"));
                    e.neighbors(this, R_rel);
                }
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (!neighbors.contains(xe) && !reversed && xe.is("S_ORDO_ORPHANET")
                && "disease_associated_with_gene".equals(props.get("name"))) {
                ObjectNode node = newJsonObject ();
                Object[] xrefs = Util.toArray(xe.payload("hasDbXref"));
                for (Object ref : xrefs) {
                    String syn = (String)ref;
                    if (syn.startsWith("HGNC:"))
                        node.put("curie", syn);
                }

                String orpha = getString (xe.payload("notation"));
                if (!node.has("curie")) {
                    logger.warning(orpha+": no HGNC found; attempting "
                                   +"to find OGG neighbors...");
                    ef.cypher(row -> {
                            Optional<String> hgnc =
                                Util.stream(row.get("xrefs"))
                                .map(Object::toString)
                                .filter(x -> x.startsWith("HGNC:"))
                                .findFirst();
                            if (hgnc.isPresent()) {
                                node.put("curie", hgnc.get());
                            }
                            else {
                                logger.warning
                                    (orpha+": no OGG neighbors found!");
                                node.put("curie", orpha);
                            }
                            return false;
                        }, "match (d:DATA)-->(n:S_ORDO_ORPHANET)--(:S_OMIM:T028)-[e]-(:S_OGG)<--(z:DATA) where d.notation='"+orpha+"' with z, count(e) as cnt return z.hasDbXref as xrefs order by cnt desc limit 1");
                }
                node.put("source_curie", notation);
                node.put("source_label", getString (xe.payload("label")));
                node.put("gene_symbol", getString (xe.payload("symbol")));
                node.put("association_type",
                         getString (props.get
                                    ("DisorderGeneAssociationType")));
                node.put("source_validation", getString
                         (props.get
                          ("DisorderGeneAssociationValidationStatus")));
                node.put("gene_sfdc_id", "");
                genes.add(node);
                neighbors.add(xe);
            }
            return true;
        }
    }

    class OMIMGenes implements NeighborVisitor {
        ArrayNode genes;
        Entity curent, bestnb;
        //Object[] values;
        Set<String> hgnc = new HashSet<>();
        Set<Entity> neighbors = new LinkedHashSet<>();
        Map<Entity, Object> values = new HashMap<>();
        
        OMIMGenes (Entity... ents) {
            this (mapper.createArrayNode(), ents);
        }
        OMIMGenes (ArrayNode genes, Entity... ents) {
            this.genes = genes;
            if (ents != null && ents.length > 0) {
                for (int i = 0; i < genes.size(); ++i) {
                    JsonNode n = genes.get(i);
                    if (n.has("curie"))
                        hgnc.add(n.get("curie").asText());
                }
                for (Entity e : ents) {
                    curent = e;
                    // molecular basis known
                    if ("3".equals(getString (e.payload("MIMTYPE")))) {
                        e.neighbors(this, I_GENE);
                        if (!values.isEmpty()) {
                            Entity[] g = values.keySet().toArray(new Entity[0]);
                            Arrays.sort(g, (a, b) -> {
                                    Object[] xa = Util.toArray(values.get(a));
                                    Object[] xb = Util.toArray(values.get(b));
                                    return xb.length - xa.length;
                                });
                            instrument (g[0]);
                        }
                    }
                }
            }
        }

        void instrument (Entity e) {
            // get hgnc from ogg
            e.neighbors((id, xe, key, reversed, props) -> {
                    if (!neighbors.contains(xe) && xe.is("S_OGG")) {
                        ObjectNode node = newJsonObject ();
                        Object[] xrefs = Util.toArray(xe.payload("hasDbXref"));
                        for (Object ref : xrefs) {
                            String syn = (String)ref;
                            if (syn.startsWith("HGNC:"))
                                node.put("curie", syn);
                        }

                        if (node.has("curie")
                            && !hgnc.contains(node.get("curie").asText())) {
                            // add only if we don't already have this hgnc
                            node.put("source_curie",
                                     getString (curent.payload("notation")));
                            node.put("source_label",
                                     getString (curent.payload("label")));
                            // ogg's label is the gene symbol
                            node.put("gene_symbol",
                                     getString (xe.payload("label")));
                            /*
                            node.put("association_type",
                                     getString (curent.payload
                                                ("MIMTYPEVALUE")));
                            */
                            node.put("association_type",
                                     "Role in the phenotype of");
                            node.put("source_validation",
                                     getString (curent.payload
                                                ("MIMTYPEMEANING")));
                            node.put("gene_sfdc_id", "");
                            genes.add(node);
                            neighbors.add(xe);
                        }
                    }
                    return true;
                }, I_CODE);
        }
            

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (xe.is("S_OMIM") && xe.is("T028")) {
                Object[] vals = Util.toArray(props.get("value"));
                Object xval = values.get(xe);
                values.put(xe, Util.merge(xval, vals));
            }
            return true;
        }
    }
    
    class OrphanetEpidemiology implements Consumer<Map<String, Object>> {
        ArrayNode epi = mapper.createArrayNode();
        String notation;
        
        OrphanetEpidemiology (Entity... ents) {
            if (ents != null && ents.length > 0) {
                epi = mapper.createArrayNode();
                for (Entity e : ents) {
                    notation = getString (e.payload("notation"));
                    e.data(this, "PREVALENCE");
                }
            }
        }

        public void accept (Map<String, Object> props) {
            ObjectNode n = newJsonObject ();
            n.put("class", (String)props.get("PrevalenceClass"));
            n.put("geographic", (String)props.get("PrevalenceGeographic"));
            n.put("qualification",(String)props.get("PrevalenceQualification"));
            n.put("type", (String)props.get("PrevalenceType"));
            Object source = props.get("Source");
            n.put("references", getString (source));
            // orphanet number
            n.put("source_curie", notation);
            Object valmoy = props.get("ValMoy");
            //n.put("valmoy", (Double)valmoy);
            n.put("valmoy", valmoy != null ? valmoy.toString():"");
            n.put("source_validation",
                  (String)props.get("PrevalenceValidationStatus"));
            epi.add(n);
        }
    }

    class OrphanetRelationships implements NeighborVisitor {
        ArrayNode neighbors = mapper.createArrayNode();
        final String relname;
        String notation;
        Set<Entity> seen = new HashSet<>();
        
        OrphanetRelationships (String relname, Entity... ents) {
            this.relname = relname;
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    notation = getString (e.payload("notation"));
                    e.neighbors(this, R_rel);
                }
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (!seen.contains(xe) && !reversed && xe.is("S_ORDO_ORPHANET")
                && relname.equals(props.get("name"))) {
                ObjectNode node = newJsonObject ();
                node.put("curie", notation);
                node.put("label", getString (xe.payload("label")));
                neighbors.add(node);
                seen.add(xe);
            }
            return true;
        }
    }

    class HPOPhenotypes implements NeighborVisitor {
        ArrayNode phenotypes = mapper.createArrayNode();
        Set<Entity> neighbors = new LinkedHashSet<>();
        ArrayNode diagnosis = mapper.createArrayNode();
        String id;
        
        HPOPhenotypes (Entity... ents) {
            if (ents != null && ents.length > 0) {
                 for (Entity e : ents) {
                     id = getString (e.payload("notation"));
                     e.neighbors(this, R_hasPhenotype);
                }
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            String hpid = (String) xe.payload("notation");
            if (!neighbors.contains(xe) && !reversed
                && xe.is("S_HP") && hpid != null && hpid.startsWith("HP:")) {
                ObjectNode node = newJsonObject ();
                node.put("curie", hpid);
                String cat = getHpCategory (hpid);
                if (cat == CATEGORY_NONE) {
                    logger.warning(id+" -> " +hpid+" ("+xe.payload("label")
                                   +") has no category!");
                }
                node.put("category", cat);
                                
                node.put("label", getString (xe.payload("label")));
                node.put("synonym",
                         getString (xe.payload("hasExactSynonym")));
                node.put("description",
                         getString (xe.payload("IAO_0000115")));
                StringBuilder freq = new StringBuilder ();
                for (Object v : Util.toArray(props.get("Frequency"))) {
                    String val = v.toString();
                    if (val.startsWith("HP:")) {
                        for (Iterator<Entity> iter = ef.find("notation", val);
                             iter.hasNext(); ) {
                            Entity e = iter.next();
                            if (freq.length() > 0) freq.append("; ");
                            freq.append(getString (e.payload("label")));
                        }
                    }
                    else {
                        if (freq.length() > 0) freq.append("; ");
                        freq.append(val);
                    }
                }
                node.put("frequency", freq.toString());
                node.put("hpo_method", getString (props.get("Evidence")));
                node.put("sex", getString (props.get("Sex")));
                node.put("source_curie", getString (props.get("Reference")));
                node.put("modifier", getString (props.get("Modifier")));
                node.put("phenotype_sfdc_id", "");

                // move this to diagnosis
                boolean diag = false;
                for (String _id : HPO_TYPE_IMAGING_MRI) {
                    if (_id.equals(hpid)) {
                        diag = true;
                        break;
                    }
                }

                if (diag) {
                    diagnosis.add(node);
                }
                else {
                    phenotypes.add(node);
                }
                neighbors.add(xe);
            }
            return true;
        }
    }

    class RanchoDrugs {
        Set<Entity> neighbors = new LinkedHashSet<>();
        Set<Entity> seen = new HashSet<>();
        ArrayNode drugs = mapper.createArrayNode();
        Entity cond;
        
        RanchoDrugs (Entity... ents) {
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    e.neighbors(this::visitCondition, N_Name, I_CODE);
                }
            }
        }

        public boolean visitCondition (long id, Entity xe, StitchKey key,
                                       boolean reversed,
                                       Map<String, Object> props) {
            if (!seen.contains(xe) && xe.is(RANCHO_LABEL)) {
                // xe is rancho condition node; now we get the drug through
                // the R_rel neighbors
                cond = xe;
                xe.neighbors(this::visitDrug, R_rel);
                seen.add(xe);
            }
            return true;
        }

        public boolean visitDrug (long id, Entity xe, StitchKey key,
                                  boolean reversed, Map<String, Object> props) {
            if (!neighbors.contains(xe)
                && "Approved".equals(props.get("HighestPhase"))
                && "indication_of".equals(props.get("value"))
                && xe.is(RANCHO_LABEL)) {
                String unii = getString (xe.payload("Unii"));
                if (!"".equals(unii)) {
                    ObjectNode node = newJsonObject ();
                    node.put("name",
                             getString (xe.payload("CompoundName")));
                    node.put("modality",
                             getString (props.get("TreatmentModality")));
                    node.put("approval_status", "ApprovedRx");
                    String date = getString
                        (cond.payload("ConditionProductDate"));
                    if (!"".equals(date)) {
                        try {
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(DF.parse(date));
                            node.put("approval_year",
                                     String.valueOf
                                     (cal.get(Calendar.YEAR)));
                        }
                        catch (Exception ex) {
                            logger.log(Level.SEVERE,
                                       "Bogus date: "+date, ex);
                        }
                    }
                    node.put("source",
                             getString (props.get("HighestPhaseUri")));
                    node.put("curie", "UNII:"+unii);
                    node.put("drug_sfdc_id", "");
                    drugs.add(node);
                    // xe is the rancho drug node
                    neighbors.add(xe);
                }
            }
            return true;
        }
    }

    class RelatedDiseases {
        Set<Entity> children = new HashSet<>();
        ArrayNode related = mapper.createArrayNode();
        
        RelatedDiseases (Entity... ents) {
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    e.inNeighbors(this::visitChild, R_subClassOf);
                }
            }
        }

        public boolean visitChild (long id, Entity child, StitchKey key,
                                   boolean reversed,
                                   Map<String, Object> props) {
            if (child.is("S_MONDO")) {
                child.neighbors(this::visitGARD,
                                R_equivalentClass, R_exactMatch);
                
            }
            return true;
        }

        public boolean visitGARD (long id, Entity xe, StitchKey key,
                                  boolean reversed, Map<String, Object> props) {
            if (xe.is("S_GARD") && !children.contains(xe)) {
                ObjectNode node = newJsonObject ();
                node.put("curie", getString (xe.payload("gard_id")));
                node.put("label", getString (xe.payload("name")));
                node.put("type", "child");
                node.put("disease_sfdc_id", "");
                related.add(node);
                children.add(xe);
            }
            return true;
        }
    }

    class DiseaseComponent {
        final Component component;
        final Map<Source, Entity[]> entities = new TreeMap<>();
        final Entity[] gard;
        final Set<Entity> genes = new LinkedHashSet<>();
        final Set<Entity> phenotypes = new LinkedHashSet<>();
        final Set<Entity> drugs = new LinkedHashSet<>();
        
        DiseaseComponent (long[] comp) {
            component = ef.component(comp);
            // partition entities into data sources
            Map<Source, Set<Entity>> map = new TreeMap<>();
            for (Entity e : component) {
                Set<String> labels = e.labels();
                for (String lbl : labels) {
                    Source src = getSource (lbl);
                    if (src != null && src.valid(e)) {
                        /*
                         * only add if this entity satisfies any additional
                         * type constraints
                         */ 
                        Set<Entity> set = map.get(src);
                        if (set == null)
                            map.put(src, set = new TreeSet<>());
                        set.add(e);
                    }
                }
            }
            
            for (Map.Entry<Source, Set<Entity>> me : map.entrySet())
                entities.put(me.getKey(), me.getValue().toArray(new Entity[0]));
            gard = get ("S_GARD");
        }

        ArrayNode toJsonArray (Object value) {
            ArrayNode ary = mapper.createArrayNode();
            if (value != null) {
                if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); ++i) {
                        Object v = Array.get(value, i);
                        ary.add(v.toString());
                    }
                }
                else {
                    ary.add(value.toString());
                }
            }
            return ary;
        }
        
        Entity[] get (String... sources) {
            List<Entity> entities = new ArrayList<>();
            for (String s : sources) {
                Entity[] ents = this.entities.get(getSource (s));
                if (ents != null && ents.length > 0)
                    entities.addAll(Arrays.asList(ents));
            }
            return entities.isEmpty() ? null : entities.toArray(new Entity[0]);
        }

        boolean has (String source) {
            return entities.containsKey(getSource (source));
        }
        
        JsonNode toJson () {
            ObjectNode disease = null;
            if (gard != null && gard.length > 0) {
                disease = mapper.createObjectNode();            
                disease.put("term", doTerm ());
                disease.put("disease_categories", doDiseaseCategories ());
                disease.put("synonyms", doSynonyms ());
                disease.put("external_identifiers", doExternalIdentifiers ());
                disease.put("inheritance", doInheritance ());
                disease.put("age_at_onset", doAgeOfOnset ());
                disease.put("age_at_death", doAgeOfDeath ());
                ArrayNode diagnosis = (ArrayNode)doDiagnoses ();
                ArrayNode phenotypes = (ArrayNode) doPhenotypes (diagnosis);
                disease.put("diagnosis", diagnosis);                
                disease.put("epidemiology", doEpidemiology ());
                disease.put("genes", doGenes ());
                disease.put("phenotypes", phenotypes);
                disease.put("drugs", doDrugs ());
                disease.put("evidence", doEvidence ());
                disease.put("related_diseases", doRelatedDiseases ());
            }
            return disease;
        }

        JsonNode doTerm () {
            ObjectNode term = newJsonObject ();
            
            Map<String, Object> data = gard[0].payload();            
            term.put("curie", (String)data.get("gard_id"));
            term.put("label", (String)data.get("name"));
            term.put("url", "https://rarediseases.info.nih.gov/diseases/"
                     +data.get("id")+"/index");
            Entity[] medgen = get ("S_MEDGEN");
            Entity[] orpha = get ("S_ORDO_ORPHANET");
            Entity[] mondo = get ("S_MONDO");

            if (medgen != null) {
                term.put("description", getString (medgen[0].payload("DEF")));
                term.put("description_curie", "MEDGEN:"
                         +getString (medgen[0].payload("id")));
                term.put("description_URL",
                         getSource("S_MEDGEN").url(medgen[0]));
            }

            JsonNode desc = term.get("description");
            if (desc != null && desc.isNull() && orpha != null) {
                term.put("description",
                         getString (orpha[0].payload("definition")));
                term.put("description_curie",
                         getString (orpha[0].payload("notation")));
                term.put("description_URL",
                         getSource("S_ORDO_ORPHANET").url(orpha[0]));
            }

            desc = term.get("description");
            if (desc != null && desc.isNull() && mondo != null) {
                term.put("description",
                         getString (mondo[0].payload("IAO_0000115")));
                term.put("description_curie",
                         getString (mondo[0].payload("notation")));
                term.put("description_URL", getSource("S_MONDO").url(mondo[0]));
            }
            
            desc = term.get("description");
            if (desc != null && desc.isNull()) {
                term.remove("description");
                term.remove("description_curie");
                term.remove("description_URL");
            }
            
            return term;
        }

        JsonNode doDiseaseCategories () {
            final Map<String, String> cats = new TreeMap<>();
            Entity[] ents = get ("S_MONDO");
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    String mondo = getString (e.payload("notation"));
                    ef.cypher(row -> {
                            cats.put((String)row.get("notation"),
                                     (String)row.get("label"));
                            return true;
                        }, String.format(MONDO_CATEGORY_FMT, mondo));
                }
            }

            ArrayNode categories = mapper.createArrayNode();
            for (Map.Entry<String, String> me : cats.entrySet()) {
                ObjectNode node = newJsonObject ();
                node.put("label", me.getValue());
                node.put("curie", me.getKey());
                node.put("category_sfdc_id", "");
                categories.add(node);
            }
            return categories;
        }
        
        JsonNode doSynonyms () {
            ArrayNode synonyms = mapper.createArrayNode();            
            for (Entity e : gard) {
                ObjectNode syn = newJsonObject ();
                syn.put("curie", getString (e.payload("gard_id")));
                syn.put("label", getString (e.payload("name")));
                synonyms.add(syn);
            }

            Map<String, Set<String>> syns = new TreeMap<>();
            for (Source src : SOURCES) {
                Entity[] ents = entities.get(src);
                if (ents != null) {
                    for (Entity e : ents) {
                        String curie = src.prefix+e.payload(src.id);
                        Set<String> unique = new HashSet<>();
                        for (String s : src.synonyms) {
                            for (Object x : Util.toArray(e.payload(s))) {
                                String sv = x.toString();
                                if (sv.length() > 256) {
                                    logger.warning(curie+": synonym is too "
                                                   +"long!\n"+sv);
                                }
                                else if (unique.contains(sv.toLowerCase())) {
                                    // don't bother synonyms that are different
                                    // based on case
                                }
                                else {
                                    Set<String> ss = syns.get(curie);
                                    if (ss == null)
                                        syns.put(curie, ss = new TreeSet<>());
                                    ss.add(sv);
                                    unique.add(sv.toLowerCase());
                                }
                            }
                        }
                    }
                }
            }

            for (Map.Entry<String, Set<String>> me : syns.entrySet()) {
                String curie = me.getKey();
                for (String s : me.getValue()) {
                    ObjectNode syn = newJsonObject ();
                    syn.put("curie", curie);
                    syn.put("label", s);
                    synonyms.add(syn);
                }
            }

            return synonyms;
        }

        JsonNode doExternalIdentifiers () {
            ArrayNode xrefs = mapper.createArrayNode();
            Map<Entity, Set<Entity>> nord = new HashMap<>();
            for (Source src : SOURCES) {
                Entity[] ents = entities.get(src);
                if (ents != null) {
                    for (Entity e : ents) {
                        ObjectNode node = newJsonObject ();
                        node.put("curie", getString (e.payload(src.id)));
                        node.put("url", src.url(e));
                        node.put("source", src.source);
                        e.neighbors((id, xe, key, reversed, props) -> {
                                if (xe.is("S_NORD")) {
                                    Set<Entity> set = nord.get(xe);
                                    if (set == null) {
                                        nord.put(xe, set = new HashSet<>());
                                    }
                                    set.add(e);
                                }
                                return true;
                            }, N_Name);
                        xrefs.add(node);
                    }
                }
            }
            if (!nord.isEmpty()) {
                Entity[] nordes = nord.keySet().toArray(new Entity[0]);
                Arrays.sort(nordes, (a, b)
                            -> nord.get(b).size() - nord.get(a).size());
                ObjectNode node = newJsonObject ();
                node.put("curie", "NORD:"+getString (nordes[0].payload("id")));
                node.put("url", getString (nordes[0].payload("url")));
                node.put("label", getString (nordes[0].payload("name")));
                node.put("source", "NORD");
                xrefs.add(node);
            }
            return xrefs;
        }
        
        JsonNode doInheritance () {
            ArrayNode inheritance = new OrphanetRelationships
                ("has_inheritance", get ("S_ORDO_ORPHANET")).neighbors;
            
            Entity[] ents = get ("S_OMIM");
            if (ents != null) {
                final Set<Entity> seen = new HashSet<>();
                for (Entity e : ents) {
                    final String notation = getString (e.payload("notation"));
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!seen.contains(xe)
                                && !reversed && xe.is("S_OMIM")
                                && "has_inheritance_type"
                                .equals(props.get("name"))) {
                                ObjectNode node = newJsonObject ();
                                node.put("curie", notation);
                                node.put("label",
                                         getString (xe.payload("label")));
                                inheritance.add(node);
                                seen.add(xe);
                            }
                            return true;
                        }, R_rel);
                }
            }
            return inheritance;
        }

        JsonNode doAgeOfOnset () {
            return new OrphanetRelationships
                ("has_age_of_onset", get ("S_ORDO_ORPHANET")).neighbors;
        }

        JsonNode doAgeOfDeath () {
            return new OrphanetRelationships
                ("has_age_of_death", get ("S_ORDO_ORPHANET")).neighbors;
        }

        JsonNode doDiagnoses () {
            ArrayNode diagnoses = mapper.createArrayNode();
            Entity[] ents = get ("S_MEDGEN");
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    boolean hasGTR = false;
                    for (Object source : Util.toArray(e.payload("SOURCES"))) {
                        if ("GTR".equals(source)) {
                            hasGTR = true;
                            break;
                        }
                    }
                    
                    if (hasGTR) {
                        ObjectNode node = newJsonObject ();
                        node.put("type", "GTR");
                        node.put("curie", "MEDGEN:"+e.payload("CUI"));
                        diagnoses.add(node);
                    }
                }
            }

            return diagnoses;
        }
        
        JsonNode doEpidemiology () {
            return new OrphanetEpidemiology(get ("S_ORDO_ORPHANET")).epi;
        }

        JsonNode doGenes () {
            OrphanetGenes og = new OrphanetGenes(get("S_ORDO_ORPHANET"));
            this.genes.addAll(og.neighbors);
            
            OMIMGenes omim = new OMIMGenes (og.genes, get ("S_OMIM"));
            this.genes.addAll(omim.neighbors);
            
            return og.genes;
        }
            
        JsonNode doPhenotypes (ArrayNode diagnosis) {
            HPOPhenotypes hp = new HPOPhenotypes
                (get ("S_OMIM", "S_ORDO_ORPHANET"));
            for (int i = 0; i < hp.diagnosis.size(); ++i) {
                ObjectNode n = (ObjectNode) hp.diagnosis.get(i);
                ObjectNode node = newJsonObject ();
                node.put("curie", n.get("curie").asText());
                node.put("type", "Imaging_MRI"); // FIXME
                diagnosis.add(node);
            }
            this.phenotypes.addAll(hp.neighbors);
            return hp.phenotypes;
        }

        JsonNode doDrugs () {
            RanchoDrugs rd = new RanchoDrugs(get ("S_DOID", "S_MESH"));
            this.drugs.addAll(rd.neighbors);
            return rd.drugs;
        }

        JsonNode doEvidence () {
            ArrayNode evidence = mapper.createArrayNode();
            Entity[] ents = get("S_GARD");
            if (ents != null) {
                Set<Entity> dups = new HashSet<>();
                for (Entity e : ents) {
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!dups.contains(xe) && xe.is("S_GENEREVIEWS")) {
                                String gr = getString (xe.payload("id"));
                                String bookid = GENEREVIEWS.get(gr);
                                if (bookid != null) {
                                    ObjectNode node = newJsonObject ();
                                    node.put("label",
                                             getString (xe.payload("title")));
                                    node.put("genes",
                                             toJsonArray (xe.payload("genes")));
                                    node.put
                                        ("url",
                                         "https://www.ncbi.nlm.nih.gov/books/"
                                         +bookid);
                                    node.put("type", "Systematic Review");
                                    evidence.add(node);
                                }
                                else {
                                    logger.warning
                                        ("Can't lookup GeneReviews "+gr);
                                }
                                dups.add(xe);
                            }
                            return true;
                        }, N_Name);
                }
            }
            return evidence;
        }

        JsonNode doRelatedDiseases () {
            RelatedDiseases rd = new RelatedDiseases (get ("S_MONDO"));
            return rd.related;
        }
    }

    public HoofBeats (EntityFactory ef) {
        this.ef = ef;
    }

    ObjectNode newJsonObject () {
        ObjectNode obj = mapper.createObjectNode();
        obj.put("Id", "");
        return obj;
    }
    
    String getHpCategory (String id) {
        String cat = hpCategories.get(id);
        if (cat == null) {
            final StringBuilder sb = new StringBuilder ();
            final String cypher = getHpCategoryCypher (id);
            ef.cypher(row -> {
                    Object label = row.get("label");
                    //logger.info("... "+id+" => "+label);
                    for (Object obj : Util.toArray(label)) {
                        String c = (String)obj;
                        if (sb.length() > 0) sb.append(";");
                        sb.append(c.replaceAll("Abnormality of the", "")
                                  .replaceAll("Abnormality of", "")
                                  .replaceAll("abnormality", "")
                                  .replaceAll("Abnormal", "").trim());
                    }
                    return label == null;
                }, cypher);
                
            if (sb.length() > 0)
                cat = sb.toString();
            else {
                cat = CATEGORY_NONE;
                //logger.warning("***** "+id+": No category for query: "+cypher);
            }
            hpCategories.put(id, cat);
        }
        return cat;
    }
    
    public void test () {
        ef.stitches((source, target, values) -> {
                try {
                    logger.info("["+source.getId()+", "+target.getId()+"] "
                                +mapper.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(values));
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, I_CODE, N_Name);
    }

    static String getIds (Component component, String source) {
        return getIds (component, getSource (source));
    }
    
    static String getIds (Component component, Source source) {
        return getIds (component, source,
                       e -> Util.stream(e.payload(source.id))
                       .map(Object::toString)
                       .collect(Collectors.toSet()));
    }

    static String getIds (Component component, String source,
                          Function<Entity, Set<String>> eval) {
        return getIds (component, getSource (source), eval);
    }
    
    static String getIds (Component component, Source source,
                          Function<Entity, Set<String>> eval) {
        StringBuilder sb = new StringBuilder ();
        Entity[] entities = filter (component, source);
        if (entities.length > 0) {
            Set<String> vals = new TreeSet<>();
            for (int i = 0; i < entities.length; ++i) {
                vals.addAll(eval.apply(entities[i]));
            }

            for (String v : vals) {
                if (sb.length() > 0) sb.append(",");
                sb.append(v);
            }
        }
        return sb.toString();        
    }

    static Entity[] filter (Component component, Source source) {
        return component.stream()
            .filter(e -> source.valid(e)).toArray(Entity[]::new);
    }

    void beats (PrintStream ps, long[] comp) throws IOException {
        Component component = ef.component(comp);
        Map<String, Integer> labels = component.labels();
        Map<Object, String> names = new TreeMap<>();
        Map<Object, String> codes = new TreeMap<>();
        Set<Long> all = new HashSet<>();
        
        Map<Object, Integer> values = component.values(N_Name);
        for (Map.Entry<Object, Integer> me : values.entrySet()) {
            long[] nodes = ef.nodes(N_Name.name(), me.getKey());
            for (int i = 0; i < nodes.length; ++i)
                all.add(nodes[i]);
            names.put(me.getKey(), me.getValue()+"/"+nodes.length);
        }
        values = component.values(I_CODE);
        for (Map.Entry<Object, Integer> me : values.entrySet()) {
            long[] nodes = ef.nodes(I_CODE.name(), me.getKey());
            for (int i = 0; i < nodes.length; ++i)
                all.add(nodes[i]);
            codes.put(me.getKey(), me.getValue()+"/"+nodes.length);
        }
        logger.info("### "+component+" "+labels
                    +" N_Name="+names+" I_CODE="+codes+"\n*** "
                    +ef.component(Util.toArray(all)));
        
        ps.print(getIds (component, "S_MONDO")+"\t");
        ps.print(getIds (component, "S_ORDO_ORPHANET")+"\t");
        ps.print(getIds (component, "S_GARD")+"\t");
        ps.print(getIds (component, "S_OMIM")+"\t");
        ps.print(getIds (component, "S_MEDGEN")+"\t");
        ps.print(getIds (component, "S_MESH")+"\t");
        ps.print(getIds (component, "S_DOID")+"\t");
        ps.print(getIds (component, "S_MEDLINEPLUS")+"\t");
        ps.print(getIds (component, "S_EFO")+"\t");
        ps.print(getIds (component, "S_ICD10CM")+"\t");
        ps.print(getIds (component, "S_MONDO", e -> 
                         Util.stream(e.payload("hasDbXref"))
                         .map(Object::toString)
                         .filter(x -> x.startsWith("MedDRA"))
                         .collect(Collectors.toSet()))+"\t");
        ps.print(getIds (component, "S_MONDO", e ->
                         Util.stream(e.payload("hasDbXref"))
                         .map(Object::toString)
                         .filter(x -> x.startsWith("SCTID"))
                         .collect(Collectors.toSet())));
        ps.println();
    }
    
    public void beats (String outfile) throws IOException {
        ef.stitches((source, target, values) -> {
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        File file = new File (outfile);
        PrintStream ps = new PrintStream (new FileOutputStream (file));
        ps.println("MONDO\tOrphanet\tGARD\tOMIM\tMedGen\tMeSH\tDOID\t"
                   +"MedLinePlus\tEFO\tICD10\tMedDRA\tSNOMEDCT");
        long[][] components = uf.components();
        for (long[] comp : components) {
            beats (ps, comp);
        }
        ps.close();
        logger.info("**** "+components.length+" components!");
    }

    public int writeJson (String base) throws IOException {
        try (PrintStream diseases = new PrintStream (new FileOutputStream
                                                     (base+"_diseases.json"));
             PrintStream genes = new PrintStream (new FileOutputStream
                                                  (base+"_genes.json"));
             PrintStream phenotypes = new PrintStream
             (new FileOutputStream (base+"_phenotypes.json"));
             PrintStream drugs = new PrintStream (new FileOutputStream
                                                  (base+"_drugs.json"))) {
            return writeJson (diseases, genes, phenotypes, drugs);
        }
    }

    void sfJsonHeader (PrintStream ps) {
        ps.println("{");
        ps.println("  \"allOrNone\": false,"); // ?
        ps.println("  \"records\": [");
    }

    void sfJsonFooter (PrintStream ps) {
        ps.println("}");
    }
    
    public int writeJson (PrintStream ps, PrintStream gps, PrintStream pps,
                          PrintStream dps) throws IOException {
        ef.stitches((source, target, values) -> {
                /*
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                */
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        int cnt = 0;
        Random rand = new Random ();
        Set<Long> genes = new HashSet<>();
        Set<Long> phenotypes = new HashSet<>();
        Set<Long> drugs = new HashSet<>();
        Map<String, JsonNode> diseases = new TreeMap<>();
    
        gps.print("[");
        pps.print("[");
        dps.print("[");
        ObjectMapper mapper = new ObjectMapper ();            
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        
        long[][] components = uf.components();
        logger.info("###### "+components.length+" components! ########");
        for (long[] comp : components) {
            DiseaseComponent dc = new DiseaseComponent (comp);
            JsonNode json = dc.toJson();
            if (json != null) {
                //if (count > 0) ps.print(",");
                //String jstr = writer.writeValueAsString(json);
                //ps.print(jstr);
                String curie = json.get("term").get("curie").asText();
                if (false && cnt < 10 && rand.nextFloat() > 0.5f) {
                    String id = curie.replaceAll(":", "_");
                    try (FileOutputStream fos =
                         new FileOutputStream (id+".json")) {
                        writer.writeValue(fos, json);
                        writeGenes (id, dc.genes.toArray(new Entity[0]));
                        writePhenotypes
                            (id, dc.phenotypes.toArray(new Entity[0]));
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    ++cnt;
                }
                ArrayNode xrefs = (ArrayNode)json.get("external_identifiers");
                for (int i = 0; i < xrefs.size(); ++i) {
                    JsonNode n = xrefs.get(i);
                    String ref = n.get("curie").asText();
                    if (ref.startsWith("GARD:")) {
                        diseases.put(ref, json);
                    }
                }
                diseases.put(curie, json);
            }

            writeEntities (gps, dc.genes, genes, this::writeGene);
            writeEntities (pps, dc.phenotypes, phenotypes, this::writePhenotype);
            writeEntities (dps, dc.drugs, drugs, this::writeDrug);
        }
        gps.println("]\n}]");
        pps.println("]\n}]");
        dps.println("]\n}]");
        
        ps.println("[");
        int count = 0;
        for (JsonNode node : diseases.values()) {
            ArrayNode related = (ArrayNode) node.get("related_diseases");
            for (int i = 0; i < related.size(); ++i) {
                ObjectNode r = (ObjectNode) related.get(i);
                JsonNode xref = diseases.get(r.get("curie").asText());
                if (xref != null) {
                    r.put("disease_sfdc_id",
                          xref.get("term").get("Id").asText());
                }
                else {
                    logger.warning("*** Disease "
                                   +node.get("term").get("curie").asText()
                                   +" references unknown disease "
                                   +r.get("curie").asText()+"!!!");
                }
            }
            if (count++ > 0)
                ps.print(",");
            ps.print(writer.writeValueAsString(node));
        }
        ps.println("]");

        logger.info("######## "+components.length+" diseases!");
        logger.info("######## "+genes.size()+" genes!");
        logger.info("######## "+phenotypes.size()+" phenotypes!");
        logger.info("######## "+drugs.size()+" drugs!");
        
        return count;
    }

    void writeEntities (PrintStream ps, Collection<Entity> entities,
                        Set<Long> processed,
                        BiConsumer<PrintStream, Entity> writer) {
        for (Entity e : entities) {
            if (!processed.contains(e.getId())) {
                if (processed.size() % BATCH_SIZE != 0) 
                    ps.println(",");
                else {
                    if (!processed.isEmpty())
                        ps.println("]\n},");
                    sfJsonHeader (ps);
                }
                writer.accept(ps, e);
                processed.add(e.getId());
            }
        }
    }

    void writeGenes (String id, Entity[] genes) throws IOException {
        writeEntities (id+"_genes.json", genes, this::writeGene);
    }

    void writeGene (PrintStream ps, Entity e) {
        String sym = "", name = "";
        StringBuilder type = new StringBuilder ();
        StringBuilder locus = new StringBuilder ();

        if (e.is("S_OGG")) {
            locus.append(getString (e.payload("OGG_0000000008")));
            type.append(getString (e.payload("OGG_0000000018")));
            sym = getString (e.payload("label"));
            name = getString (e.payload("OGG_0000000005"));
        }
        else if (e.is("S_ORDO_ORPHANET")) {
            sym = getString (e.payload("symbol"));
            name = getString (e.payload("label"));
            Set<Entity> seen = new HashSet<>();
            e.neighbors((id, xe, key, reversed, props) -> {
                    if (!seen.contains(xe)
                        && "hasDbXref".equals(props.get("name"))
                        && xe.is("S_OGG")
                        && props.get("value").toString().startsWith("HGNC:")) {
                        locus.append(getString (xe.payload("OGG_0000000008")));
                        type.append(getString (xe.payload("OGG_0000000018")));
                        seen.add(xe);
                    }
                    return true;
                }, I_CODE);
        }
        Optional<String> hgnc = Util.stream(e.payload("hasDbXref"))
            .map(Object::toString)
            .filter(x -> x.startsWith("HGNC:"))
            .findFirst();
        String id = hgnc.isPresent()
            ? hgnc.get() : getString (e.payload("notation"));
        
        ps.println("    {");
        ps.println("        \"attributes\": {");
        ps.println("            \"type\": \"Gene__c\"");
        ps.println("         },");
        ps.println("        \"Name\": \""+sym+"\",");
        ps.println("        \"Gene_Name__c\": \""+name+"\",");
        ps.println("        \"GHR_URL__c\": \"https://medlineplus.gov/genetics/gene/"+sym.toLowerCase()+"\",");
        ps.println("        \"Gene_Type__c\": \""+type+"\",");        
        ps.println("        \"Chromosome_Location__c\": \""+locus+"\",");
        ps.println("        \"Gene_Identifier__c\": \""+id+"\"");
        ps.print  ("    }");
    }

    void writePhenotypes (String id, Entity[] phenotypes)
        throws IOException {
        writeEntities (id+"_phenotypes.json", phenotypes, this::writePhenotype);
    }

    void writePhenotype (PrintStream ps, Entity e) {
        ps.println("    {");
        ps.println("        \"attributes\": {");
        ps.println("            \"type\": \"Feature__c\"");
        ps.println("         },");
        ps.println("        \"Name\": \""
                   +getString (e.payload("notation"))+"\",");
        ps.println("        \"HPO_Name__c\": \""
                   +getString (e.payload("label"))+"\",");
        ps.println("        \"HPO_Synonym__c\": \""
                   +getString (e.payload("hasExactSynonym"))+"\",");
        String hpid = (String) e.payload("notation");
        String cat = getHpCategory (hpid);
        if (cat == CATEGORY_NONE) {
            logger.warning(hpid+": can't find phenotype category!");
        }
        ps.println("        \"HPO_Category__c\": \""+cat+"\",");
        ps.println("        \"HPO_Description__c\": \""+
                   getString (e.payload("IAO_0000115"))+"\",");
        ps.println("        \"HPO_Feature_URL__c\": \""
                   +"https://hpo.jax.org/app/browse/term/"+hpid+"\",");
        ps.println("        \"External_ID__c\": \""
                   +getString (e.payload("notation"))+"\"");
        ps.print("    }");
    }

    void writeDrugs (String id, Entity[] drugs) throws IOException {
        writeEntities (id+"_drugs.json", drugs, this::writeDrug);
    }

    void writeDrug (PrintStream ps, Entity e) {
        ps.println("    {");
        ps.println("        \"attributes\": {");
        ps.println("            \"type\": \"Drug__c\"");
        ps.println("         },");
        String unii = getString (e.payload("Unii"));
        ps.println("        \"Name\": \""+unii+"\",");
        ps.println("        \"Drug_Name__c\": \""
                   +getString (e.payload("CompoundName"))+"\",");
        ps.println("        \"Drug_Identifier__c\": \"UNII:"+unii+"\",");
        final Set<String> syns = new TreeSet<>();
        e.neighbors((id, xe, key, reversed, props) -> {
                if ("Approved".equals(props.get("HighestPhase"))
                    && "indication_of".equals(props.get("value"))
                    && xe.is(RANCHO_LABEL)) {
                    syns.add(getString (xe.payload("ConditionProductName")));
                }
                return true;
            }, R_rel);
        ps.println("        \"Drug_Synonyms__c\": \""
                   +getString (syns.toArray(new String[0]))+"\"");
        ps.print  ("    }");
    }

    void writeEntities (String file, Entity[] entities,
                        BiConsumer<PrintStream, Entity> writeEntity)
        throws IOException {
        try (FileOutputStream fos = new FileOutputStream (file)) {
            PrintStream ps = new PrintStream (fos);
            ps.println("{");
            ps.println("  \"allOrNone\": false,"); // ?
            ps.println("  \"records\": [");
            for (int i = 0; i < entities.length; ++i) {
                Entity e = entities[i];
                if (i > 0) {
                    ps.println(",");
                }
                writeEntity.accept(ps, e);
            }
            ps.println("]");
            ps.println("}");
        }
    }

    public void untangle (EntityFactory ef, BiConsumer<Long, long[]> consumer) {
        long[][] components = uf.components();
        logger.info("There are "+components.length
                    +" components after merging!");
        for (int i = 0; i < components.length; ++i) {
            long[] comp = components[i];
            logger.info("generating component "
                        +(i+1)+"/"+components.length+"...");
            long root = 0;
            if (comp.length == 1)
                root = comp[0];
            else {
            }
            consumer.accept(root, comp);
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+HoofBeats.class.getName()+" DBDIR");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            HoofBeats hb = new HoofBeats (ef);
            int pos = argv[0].indexOf('.');
            if (pos < 0)
                pos = argv[0].length();
            String base = argv[0].substring(0, pos);
            hb.beats(base+"_mappings.txt");
            hb.writeJson(base);
        }
    }
}
