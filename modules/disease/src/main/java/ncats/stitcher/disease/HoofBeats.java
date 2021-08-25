package ncats.stitcher.disease;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.stream.Collectors;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;
import java.text.SimpleDateFormat;
import javax.xml.xpath.*;
import javax.xml.parsers.*;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Document;

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
            case "S_MEDLINEPLUS":
                { String url = getString (e.payload("MP_HEALTH_TOPIC_URL"), 0);
                    if (url == null || url.equals(""))
                        url = getString (e.payload("uri"), 0);
                    return url;
                }
            case "S_GHR":
                return getString (e.payload("ghr-page"), 0);
            }
            return getString (e.payload("uri"), 0);
        }

        public String curie (Entity e) {
            switch (name) {
            case "S_GHR":
                for (Object r : Util.toArray(e.payload("xrefs"))) {
                    String s = r.toString();
                    if (s.startsWith("UMLS"))
                        return s;
                }
                break;
            case "S_MEDGEN":
                { String curie = getString (e.payload(id));
                    if (curie.startsWith("CN"))
                        return "MEDGEN:"+curie;
                    return "UMLS:"+curie;
                }
            }
            return getString (e.payload(id));
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
        public boolean sourceOf (Entity e) {
            return e.is(name);
        }
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
        SOURCES.add(new Source ("S_GHR", "id", "", "name",
                                "MedlinePlusGenetics", "synonyms"));
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

    static final String HP_TYPE_FMT = "match (d:DATA)-->(n:S_HP)-"
        +"[e:R_subClassOf*0..13]->(:S_HP)--(z:DATA) "
        +"where d.notation='%1$s' and all(x in e "
        +"where x.source=n.source or n.source in x.source) "
        +"and z.notation='%2$s' return d.label as label, "
        +"d.notation as notation";

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

    static final String HP_INHERITANCE = "match (d:DATA)-[:PAYLOAD]->(n:S_HP)-[e:R_subClassOf*0..]->(:S_HP)<-[:PAYLOAD]-(z:DATA) where all(x in e where x.source=n.source or n.source in x.source) and z.notation='HP:0000005' return d.notation as notation, d.label as label, d.IAO_0000115 as description";
    
    static final String CATEGORY_NONE = "";

    enum HP_Type {        
        Inheritance ("HP:0000005"),
        Onset ("HP:0003674"),
        Mortality ("HP:0040006"),
        Progression ("HP:0003679"),
        
        Imaging_MRI ("HP:0500016", "HP:0012696", "HP:0040272",
                     "HP:0012747", "HP:0002419", "HP:0012751",
                     "HP:0032615", "HP:0007103", "HP:0030890",
                     "HP:0040331", "HP:0040328", "HP:0040333",
                     "HP:0040330", "HP:0041056", "HP:0002454",
                     "HP:0033048", "HP:0033049", "HP:0030775",
                     "HP:0032270", "HP:0012705", "HP:0032268",
                     "HP:0005932", "HP:0032270"),
        
        Imaging_PET ("HP:0012657", "HP:0012657"),
        Imaging_CT ("HP:0025389", "HP:0025389", "HP:0025389", "HP_0032070",
                    "HP:0031983", "HP: 0025179", "HP: 0032267", "HP:0005932",
                    "HP:0032270"),
        Imaging_Ultrasound ("HP:0005932"),
        Imaging_Ultrasound_Fetal ("HP:0011425", "HP:0010956", "HP:0010880",
                                  "HP:0010942", "HP:0010948"),
        Imaging_Echocardiogram ("HP:0010942", "HP:0003116", "HP:0010948"),

        Procedure_EKG ("HP:0003115"),
        Procedure_EMG ("HP:0003457", "HP:0003730", "HP:0100288",
                       "HP:0003458", "HP:0033580"),
        Procedure_NCV ("HP:0003134"),
        Procedure_EEG ("HP:0002353"),
        Procedure_PFT ("HP:0030878"),

        // include child concepts except when they also belong to
        // other categories        
        Lab ("HP:0001939"),
        
        Symptom;

        final Set<String> nodes = new HashSet<>();
        HP_Type (String... nodes) {
            for (String n : nodes)
                this.nodes.add(n);
        }
        public boolean has (String n) { return nodes.contains(n); }
        public boolean isEmpty () { return nodes.isEmpty(); }
        public boolean isType (EntityFactory ef, String id) {
            if (nodes.contains(id))
                return true;
            
            Set found = new HashSet ();
            for (String n : nodes) {
                ef.cypher(row -> {
                        Object label = row.get("label");
                        if (label != null)
                            found.add(label);
                        return false;
                    }, String.format(HP_TYPE_FMT, id, n));
                if (!found.isEmpty())
                    return true;
            }
            return false;
        }
    }

    static class Specialty {
        final Map<String, String> parents = new HashMap<>();
        final String curie;
        Specialty (EntityFactory ef, String curie,
                    String source, String... concepts) {
            this.curie = curie;
            for (String s : concepts) {
                ef.cypher(row -> {
                        long id = (Long)row.get("id");
                        for (Entity[] path : ef.children(id, R_subClassOf)) {
                            for (Entity e : path) {
                                String n = getString (e.payload("notation"));
                                String old = parents.put(n, s);
                                if (old != null && !old.equals(s)) {
                                    logger.warning("** "+n
                                                   +" has both parents: "
                                                   +s+" "+old);
                                }
                            }
                        }
                        return false;
                    }, "match (d)-[:PAYLOAD]->(n:`"+source
                    +"`) where d.notation='"+s+"' return id(n) as id");
                
            }
            
            //logger.info(">>>> Specialty..."+parents);
        }

        public String getClass (String id) {
            return parents.get(id);
        }
    }

    static class Category {
        final Map<Long, Object> categories = new TreeMap<>();
        final EntityFactory ef;
        Category (EntityFactory ef, String source, String...curies) {
            for (String curie : curies) {
                ef.cypher(row -> {
                        long id = (Long) row.get("id");
                        String label = (String) row.get("label");
                        String notation = (String) row.get("notation");
                        categories.put(id, new String[]{notation, label});
                        return true;
                    }, "match (d)-[:PAYLOAD]->(n:`"+source
                    +"`) where d.notation='"+curie
                    +"' return id(n) as id, d.label as label, "
                    +"d.notation as notation");
            }
            this.ef = ef;
        }

        public int categorize (Entity e, BiConsumer<String, String> consumer) {
            int ncats = 0;
            for (Entity[] parents : ef.parents(e.getId(), R_subClassOf)) {
                for (Entity p : parents) {
                    String[] cat = (String[])categories.get(p.getId());
                    if (cat != null) {
                        consumer.accept(cat[0], cat[1]);
                        ++ncats;
                    }
                }
            }
            return ncats;
        }
    }
    
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

    static Source getSource (Entity e) {
        for (Source s : SOURCES)
            if (s.sourceOf(e))
                return s;
        return null;
    }

    static String getId (Entity e) {
        Source s = getSource (e);
        return s != null ? s.curie(e) : null;
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
        return getString (value, 255);
    }
    
    static String getString (Object value, int max) {
        if (value != null) {
            if (value.getClass().isArray()) {
                StringBuilder sb = new StringBuilder ();
                for (int i = 0; i < Array.getLength(value); ++i) {
                    String s = (String)Array.get(value, i);
                    int len = s.length();
                    if (len > 0) {
                        if (max > 0 && sb.length() + len+ 2 >= max) {
                            // truncate string
                            sb.append("...");
                            break;
                        }
                        else {
                            if (sb.length() > 0) sb.append("; ");
                            sb.append(s);
                        }
                    }
                }
                value = sb.toString();
            }
            
            String v = value.toString();
            if (max > 0 && v.length() > max) {
                v = v.substring(0, max-3) + "...";
            }
            
            return escape (v);
        }
        return "";
    }
    
    final EntityFactory ef;    
    final UnionFind uf = new UnionFind ();
    final ObjectMapper mapper = new ObjectMapper ();
    final Map<String, String> hpCategories = new HashMap<>();
    final Map<String, HP_Type[]> hpTypes = new HashMap<>();
    final Map<String, Specialty[]> specialties = new HashMap<>();

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

        OMIMGenes (final PrintStream ps) {
            this.genes = mapper.createArrayNode();
            ef.cypher(row -> {
                    Entity e = ef.getEntity((Long)row.get("id"));
                    instrument (e);
                    return true;
                }, "match (d)-[:PAYLOAD]->(n:S_OMIM:T028) "
                +"where d.MIMTYPE ='1' return id(n) as id");
            writeEntities (ps, neighbors, new HashSet<>(),
                           HoofBeats.this::writeGene);
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
                            if (curent != null) {
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
                            }
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
            this (null, relname, ents);
        }
        
        OrphanetRelationships (Set<Entity> nodes,
                               String relname, Entity... ents) {
            this.relname = relname;
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    notation = getString (e.payload("notation"));
                    e.neighbors(this, R_rel);
                }
                if (nodes != null)
                    nodes.addAll(seen);
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (!seen.contains(xe) && !reversed && xe.is("S_ORDO_ORPHANET")
                && relname.equals(props.get("name"))) {
                ObjectNode node = newJsonObject ();
                node.put("curie", notation);
                node.put("label", getString (xe.payload("label"), 4000));
                neighbors.add(node);
                seen.add(xe);
            }
            return true;
        }
    }

    class HPOPhenotypes implements NeighborVisitor {
        final ArrayNode phenotypes = mapper.createArrayNode();
        final ArrayNode inheritance = mapper.createArrayNode();
        final ArrayNode onset = mapper.createArrayNode();
        final ArrayNode mortality = mapper.createArrayNode();
        final Set<Entity> neighbors = new LinkedHashSet<>();
        final Map<HP_Type, Set<Entity>> phenos;
        String id;
        
        HPOPhenotypes (Map<HP_Type, Set<Entity>> phenos, Entity... ents) {
            this.phenos = phenos;            
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    id = getString (e.payload("notation"));
                    e.neighbors(this, R_hasPhenotype);
                }
            }
        }

        HPOPhenotypes (final PrintStream ps) {
            this.phenos = null;
            ef.cypher(row -> {
                    Entity e = ef.getEntity((Long)row.get("id"));
                    neighbors.add(e);
                    return true;
                }, "match (d)-[:PAYLOAD]->(n:S_HP) "
                +"where d.hasOBONamespace='human_phenotype' "
                +"return id(n) as id");
            writeEntities (ps, neighbors, new HashSet<>(),
                           HoofBeats.this::writePhenotype);
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            String hpid = (String) xe.payload("notation");
            if (!neighbors.contains(xe) && !reversed
                && xe.is("S_HP") && hpid != null && hpid.startsWith("HP:")) {
                Object deprecated = xe.payload("deprecated");
                if (deprecated != null
                    && deprecated.toString().equalsIgnoreCase("true")) {
                    logger.warning(id+" -> " +hpid+" ("+xe.payload("label")
                                   +") is deprecated; not include!");
                }
                else {
                    HP_Type[] types = getHpTypes (hpid);
                    for (HP_Type t : types) {
                        switch (t) {                            
                        case Inheritance:
                            inheritance.add
                                (createNode
                                 (xe, getString (props.get("Reference"))));
                            break;
                            
                        case Onset:
                            onset.add(createNode (xe, hpid));
                            break;
                            
                        case Mortality:
                            mortality.add(createNode (xe, hpid));
                            break;

                            //case Symptom:
                            //case Progression: // <-- really?
                        default:
                            phenotypes.add(createSymptomNode
                                           (xe, hpid, props));
                            break;
                        }
                        
                        Set<Entity> ents = phenos.get(t);
                        if (ents == null)
                            phenos.put(t, ents = new HashSet<>());
                        ents.add(xe);
                    }
                    neighbors.add(xe);
                }
            }
            return true;
        }

        ObjectNode createNode (Entity xe, String hpid) {
            ObjectNode n = newJsonObject ();
            n.put("curie", hpid);
            n.put("label", getString (xe.payload("label")));
            return n;
        }
                    
        ObjectNode createSymptomNode (Entity xe, String hpid,
                                      Map<String, Object> props) {
            ObjectNode node = newJsonObject ();
            node.put("curie", hpid);
            String cat = getHpCategory (hpid);
            if (cat == CATEGORY_NONE) {
                logger.warning(id+" -> " +hpid+" ("+xe.payload("label")
                               +") has no category!");
            }
            node.put("category", cat);
            
            node.put("label", getString (xe.payload("label")));
            node.put("synonym", getString (xe.payload("hasExactSynonym"), 0));
            node.put("description",
                     getString (xe.payload("IAO_0000115"), 0));
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
            return node;
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
                             getString (xe.payload("CompoundName"), 0));
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

        final GARDOntology gardOnt;
        GARDOntology.GARDClass gardClass;
        
        DiseaseComponent (GARDOntology gardOnt, long[] comp) {
            this.gardOnt = gardOnt;
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
            ObjectNode disease = mapper.createObjectNode();
            if (gard != null && gard.length > 0) {
                disease.put("term", doTerm ());
            }

            disease.put("disease_categories", doDiseaseCategories ());
            disease.put("synonyms", doSynonyms ());
            disease.put("external_identifiers", doExternalIdentifiers ());
            
            Map<HP_Type, Set<Entity>> phenos = new HashMap<>();
            phenos.put(HP_Type.Inheritance, new HashSet<>());
            phenos.put(HP_Type.Onset, new HashSet<>());
            
            ArrayNode inheritance =
                (ArrayNode) doInheritance (phenos.get(HP_Type.Inheritance));
            ArrayNode onset = (ArrayNode) doAgeAtOnset
                (phenos.get(HP_Type.Onset));
            ArrayNode mortality = (ArrayNode) doAgeAtDeath ();
            ArrayNode phenotypes = (ArrayNode) doPhenotypes
                (inheritance, onset, mortality, phenos);
            disease.put("inheritance", inheritance);
            disease.put("age_at_onset", onset);
            disease.put("age_at_death", mortality);
            
            disease.put("diagnosis", doDiagnoses ());
            disease.put("epidemiology", doEpidemiology ());
            disease.put("genes", doGenes ());
            disease.put("phenotypes", phenotypes);
            disease.put("drugs", doDrugs ());
            disease.put("evidence", doEvidence ());
            disease.put("related_diseases", doRelatedDiseases ());
            disease.put("tags", doTags (phenos));

            if (!disease.has("term")) {
                logger.info("#################\n"+jsonString (disease));
                if (phenotypes.size() > 0
                    || inheritance.size() > 0
                    || disease.get("genes").size() > 0
                    || disease.get("epidemiology").size() > 0) {
                    logger.info(":::::: NEW DISEASE FOR COMPONENT "
                                +entities.size()+"="+entities.keySet()
                                +" "+component);
                }
                disease = null;                
            }
            
            return disease;
        }

        String jsonString (Object obj) {
            try {
                return mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(obj);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't write json", ex);
            }
            return "";
        }

        JsonNode doTerm () {
            ObjectNode term = newJsonObject ();
            
            Map<String, Object> data = gard[0].payload();
            String id = (String)data.get("gard_id");
            term.put("curie", id);
            term.put("label", (String)data.get("name"));
            term.put("url", "https://rarediseases.info.nih.gov/diseases/"
                     +data.get("id")+"/index");
            Entity[] medgen = get ("S_MEDGEN");
            Entity[] orpha = get ("S_ORDO_ORPHANET");
            Entity[] mondo = get ("S_MONDO");
            Entity[] doid = get ("S_DOID");

            if (gardSummaries.containsKey(id)) {
                Map<String, String> d = gardSummaries.get(id);
                String name = d.get("DiseaseName");
                term.put("label", name);
                String text = d.get("SummaryText");
                if (text != null)
                    text = text.replaceAll("DiseaseName", name);
                term.put("description", text);
                term.put("description_curie", id);
                term.put("description_URL", term.get("url").asText());
            }

            if (instrumentTerm (term, "S_MEDGEN", "DEF", "id"))
                ;
            else if (instrumentTerm (term, "S_ORDO_ORPHANET",
                                     "definition", "notation"))
                ;
            else if (instrumentTerm (term, "S_MONDO",
                                     "IAO_0000115", "notation"))
                ;
            else if (instrumentTerm (term, "S_DOID",
                                     "IAO_0000115", "notation"))
                ;

            gardClass =
                gardOnt.createDisease(id, term.get("url").asText())
                .addLabel(term.get("label").asText())
                ;
            
            JsonNode desc = term.get("description");
            if (desc != null && desc.isNull()) {
                term.remove("description");
                term.remove("description_curie");
                term.remove("description_URL");
            }
            else if (desc != null) {
                gardClass.addDescription(desc.asText());
            }
            
            return term;
        }

        boolean instrumentTerm (ObjectNode term, String source,
                             String descprop, String idprop) {
            JsonNode desc = term.get("description");
            boolean instru = false;
            if (desc != null && desc.isNull()) {
                Entity[] ents = get (source);
                if (ents != null && ents.length > 0) {
                    term.put("description",
                             getString (ents[0].payload(descprop), 0));
                    term.put("description_curie",
                             getString (ents[0].payload(idprop)));
                    term.put("description_URL",
                             getSource(source).url(ents[0]));
                    instru = true;
                }
            }
            return instru;
        }

        JsonNode doDiseaseCategories () {
            final Map<String, String> cats = new TreeMap<>();
            Entity[] ents = get ("S_MONDO");
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    /*
                    String mondo = getString (e.payload("notation"));
                    ef.cypher(row -> {
                            cats.put((String)row.get("notation"),
                                     (String)row.get("label"));
                            return true;
                        }, String.format(MONDO_CATEGORY_FMT, mondo));
                    */
                    mondoCategory.categorize(e, (notation, label) -> {
                            cats.put(notation, label);
                        });
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
            if (gard != null) {
                for (Entity e : gard) {
                    ObjectNode syn = newJsonObject ();
                    syn.put("curie", getString (e.payload("gard_id")));
                    syn.put("label", getString (e.payload("name"), 0));
                    synonyms.add(syn);
                }
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
                Set<String> synlist = me.getValue();
                /*
                if (!gardSynonymsWhitelist.isEmpty()
                    && gardSynonymsWhitelist.containsKey(curie)) {
                    // override
                    synlist = gardSynonymsWhitelist.get(curie);
                    logger.info("## overriding "+curie+" with whitelist: "
                                +syns.size());
                }
                */

                for (String s : synlist) {
                    ObjectNode syn = newJsonObject ();
                    syn.put("curie", curie);
                    syn.put("label", s);
                    if (gardClass != null) {
                        if (curie.startsWith("GARD")) {
                            gardClass.addSynonym(s);
                        }
                        else { // add axiom here for this source?
                        }
                    }
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
                        node.put("curie", src.curie(e));
                        node.put("url", src.url(e));
                        node.put("source", src.source);
                        // FIXME: this should be done when we create
                        // components similar to S_GHR!
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
                // use nord entry that has the most mappings to this component
                Entity[] nordes = nord.keySet().toArray(new Entity[0]);
                Arrays.sort(nordes, (a, b)
                            -> nord.get(b).size() - nord.get(a).size());
                ObjectNode node = newJsonObject ();
                // nord doesn't have a curie
                node.put("curie", "NORD:"
                         +getString(nordes[0].payload("id"), 0)
                         .toLowerCase().replaceAll("\\s", "-"));
                node.put("url", getString (nordes[0].payload("url")));
                //node.put("label", getString (nordes[0].payload("name")));
                node.put("source", "NORD");
                xrefs.add(node);
            }
            return xrefs;
        }

        JsonNode doInheritance () {
            return doInheritance (null);
        }
        
        JsonNode doInheritance (Set<Entity> inheritanceNodes) {
            OrphanetRelationships orphan = new OrphanetRelationships
                ("has_inheritance", get ("S_ORDO_ORPHANET"));
            
            ArrayNode inheritance = orphan.neighbors;
            if (inheritanceNodes != null)
                inheritanceNodes.addAll(orphan.seen);
            
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
                                         getString (xe.payload("label"), 0));
                                inheritance.add(node);
                                seen.add(xe);
                            }
                            return true;
                        }, R_rel);
                }
                
                if (inheritanceNodes != null)
                    inheritanceNodes.addAll(seen);
            }
            return inheritance;
        }

        JsonNode doAgeAtOnset (Set<Entity> nodes) {
            return new OrphanetRelationships
                (nodes, "has_age_of_onset", get ("S_ORDO_ORPHANET")).neighbors;
        }

        JsonNode doAgeAtDeath () {
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

            Set<Entity> newborn = new HashSet<>();
            for (Entity[] ez : entities.values()) {
                for (Entity e : ez) {
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (xe.is("S_NEWBORN"))
                                newborn.add(xe);
                            return true;
                        }, I_CODE, N_Name);
                }
            }
            
            for (Entity e : newborn) {
                ObjectNode node = newJsonObject ();
                node.put("type", "NEWBORN");
                node.put("curie", getString (e.payload("web-page")));
                node.put("category", getString (e.payload("sachdnc-cat")));
                diagnoses.add(node);
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

        JsonNode doPhenotypes (ArrayNode inheritance,
                               ArrayNode onset, ArrayNode mortality,
                               Map<HP_Type, Set<Entity>> phenos) {
            HPOPhenotypes hp = new HPOPhenotypes
                (phenos, get ("S_OMIM", "S_ORDO_ORPHANET"));
            for (int i = 0; i < hp.inheritance.size(); ++i)
                inheritance.add(hp.inheritance.get(i));
            for (int i = 0; i < hp.onset.size(); ++i)
                onset.add(hp.onset.get(i));
            for (int i = 0; i < hp.mortality.size(); ++i)
                mortality.add(hp.mortality.get(i));
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
                                             getString (xe.payload("title"),
                                                        0));
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

        JsonNode doTags (Map<HP_Type, Set<Entity>> phenotypes) {
            ArrayNode nodes = mapper.createArrayNode();
            // first generate inheritance tags=
            Set<Entity> inheritance = phenotypes.get(HP_Type.Inheritance);
            if (inheritance != null) {
                Set<String> matches = new HashSet<>();                
                for (Entity e : inheritance) {
                    String curie = null;
                    if (e.is("S_OMIM")) {
                        curie = getString (e.payload("cui"));
                    }
                    else if (e.is("S_ORDO_ORPHANET")) {
                        curie = getString (e.payload("notation"));
                    }
                    
                    if (curie != null) {
                        JsonNode hpnode = hpInheritance.get(curie);
                        if (hpnode != null) {
                            String hpid = hpnode.get("curie").asText();
                            if (!matches.contains(hpid)) {
                                logger.info
                                    ("##### "+curie+" ("+e.payload("label")
                                     +") => "+hpnode.get("curie").asText()
                                     +" ("+hpnode.get("label").asText()+")");
                                nodes.add(hpnode);
                                matches.add(hpid);
                            }
                        }
                        else {
                            logger.warning("**** inheritance "+curie
                                           +" ("+getString (e.payload("label"))
                                           +") not mapped!");
                        }
                    }
                }
            }

            Set<Entity> allents = new HashSet<>();
            // now for specialties based on mondo
            Entity[] ents = get ("S_MONDO");
            if (ents != null && ents.length > 0) {
                for (Entity e : ents)
                    allents.add(e);
            }

            // specialties based on phenotypes
            Set<Entity> set = phenotypes.get(HP_Type.Symptom);
            if (set != null) {
                allents.addAll(set);
            }

            Map<String, String> tags = new LinkedHashMap<>();
            for (Entity e : allents) {
                String id = getString (e.payload("notation"));
                for (Map.Entry<String, Specialty[]> me
                         : specialties.entrySet()) {
                    for (Specialty sub : me.getValue()) {
                        String clz = sub.getClass(id);
                        if (null != clz) {
                            tags.put(me.getKey(), sub.curie);
                            break;
                        }
                    }
                }
            }
            
            // specialties based on age of onset
            Set<Entity> onset = phenotypes.get(HP_Type.Onset);
            if (onset != null) {
                Set<String> curies = new TreeSet<>();
                for (Entity e : onset) {
                    String id = getString (e.payload("notation"));
                    switch (id) {
                    case "Orphanet:409943": // antenatal
                    case "Orphanet:409944": // neonatal
                    case "Orphanet:409945": // infancy
                    case "Orphanet:409946": // childhood
                    case "Orphanet:409947": // adolescent
                        curies.add(id);
                        break;
                    }
                }
                if (!curies.isEmpty())
                    tags.put("Pediatrics", "GARD:S020");
            }

            for (Map.Entry<String, String> me : tags.entrySet()) {
                ObjectNode node = newJsonObject ();
                node.put("curie", me.getValue());
                node.put("label", me.getKey());
                node.put("category", "Disease Ontology");
                node.put("tag_sfdc_id", "");
                nodes.add(node);
            }
            
            return nodes;
        }
    }

    Map<String, String> ghrUrls = new HashMap<>();
    Map<String, Map<String, String>> gardSummaries = new HashMap<>();
    Map<String, JsonNode> hpInheritance = new HashMap<>();
    Category mondoCategory;

    final GARDOntology gardOnt;

    public HoofBeats (EntityFactory ef) throws Exception {
        this.ef = ef;
        loadGARDSummaries ();
        loadHpInheritance ();
        setupSpecialties ();
        downloadGHRUrls ();

        mondoCategory = new Category (ef, "S_MONDO", new String[]{
                "MONDO:0003900", "MONDO:0021147", "MONDO:0045024",
                "MONDO:0024297", "MONDO:0045028", "MONDO:0000839",
                "MONDO:0024236", "MONDO:0021178", "MONDO:0021166",
                "MONDO:0005550", "MONDO:0003847", "MONDO:0020683",
                "MONDO:0002254", "MONDO:0002025", "MONDO:0020012",
                "MONDO:0021669", "MONDO:0024575", "MONDO:0002409",
                "MONDO:0004995", "MONDO:0004335", "MONDO:0021145",
                "MONDO:0024458", "MONDO:0005151", "MONDO:0005570",
                "MONDO:0005046", "MONDO:0002051", "MONDO:0002081",
                "MONDO:0005071", "MONDO:0005087", "MONDO:0002118",
                "MONDO:0100120", "MONDO:0005135", "MONDO:0019751",
                "MONDO:0007179", "MONDO:0004992", "MONDO:0024674",
                "MONDO:0024882", "MONDO:0021058", "MONDO:0100120",
                "MONDO:0005135", "MONDO:0019052", "MONDO:0044970",
                "MONDO:0005365", "MONDO:0019512", "MONDO:0005267",
                "MONDO:0005385", "MONDO:0015111", "MONDO:0005020",
                "MONDO:0006858", "MONDO:0002356", "MONDO:0002332",
                "MONDO:0004298", "MONDO:0002263", "MONDO:0005047",
                "MONDO:0003150", "MONDO:0005328", "MONDO:0002135",
                "MONDO:0021084", "MONDO:0001941", "MONDO:0005495",
                "MONDO:0015514", "MONDO:0005154", "MONDO:0100070",
                "MONDO:0002356", "MONDO:0001223", "MONDO:0003393",
                "MONDO:0003240", "MONDO:0002280", "MONDO:0001531",
                "MONDO:0002245", "MONDO:0003804", "MONDO:0003225",
                "MONDO:0044348", "MONDO:0017769", "MONDO:0005271",
                "MONDO:0009453", "MONDO:0003778", "MONDO:0005093",
                "MONDO:0024255", "MONDO:0006607", "MONDO:0006615",
                "MONDO:0019296", "MONDO:0005218", "MONDO:0005172",
                "MONDO:0023603", "MONDO:0002602", "MONDO:0005560",
                "MONDO:0005027", "MONDO:0005559", "MONDO:0002320",
                "MONDO:0005287", "MONDO:0019117", "MONDO:0020010",
                "MONDO:0005395", "MONDO:0003620", "MONDO:0100081",
                "MONDO:0000270", "MONDO:0005275", "MONDO:0000376",
                "MONDO:0004867", "MONDO:0005240", "MONDO:0006026",
                "MONDO:0020059", "MONDO:0007254", "MONDO:0008170",
                "MONDO:0001657", "MONDO:0006517", "MONDO:0021063",
                "MONDO:0005575", "MONDO:0008903", "MONDO:0002120",
                "MONDO:0005206", "MONDO:0002898", "MONDO:0004950",
                "MONDO:0001627", "MONDO:0006999", "MONDO:0044872",
                "MONDO:0003441", "MONDO:0019956", "MONDO:0020640",
                "MONDO:0020067", "MONDO:0001835", "MONDO:0001071",
                "MONDO:0000437", "MONDO:0005258", "MONDO:0017713",
                "MONDO:0016054", "MONDO:0018075", "MONDO:0008449",
                "MONDO:0019716", "MONDO:0002561", "MONDO:0020121"
            });

        gardOnt = new GARDOntology ();
    }

    void loadGARDSummaries () {
        try {
            File file = new File ("GARD_Summaries.tsv");
            if (file.exists()) {
                try (BufferedReader br = new BufferedReader
                     (new FileReader (file))) {
                    // skip header: 
                    String[] header = br.readLine().split("\\t");
                    for (String line; (line = br.readLine()) != null; ) {
                        String[] toks = line.split("\\t");                     
                        Map<String, String> data = new HashMap<>();
                        for (int i = 0; i < toks.length; ++i) {
                            data.put(header[i], toks[i]);
                        }
                        //logger.info("==> "+data);
                        String id = data.remove("DiseaseID");
                        gardSummaries.put(id, data);
                    }
                }
                logger.info("## "+gardSummaries.size()
                            +" GARD summaries loaded!");
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't load GARD synonyms whitelist!", ex);
        }
    }

    void loadHpInheritance () {
        try {
            Map<String, JsonNode> hpmap = new HashMap<>();
            ef.cypher(row -> {
                    ObjectNode node = newJsonObject ();
                    String curie = (String)row.get("notation");
                    node.put("curie", curie);
                    node.put("label", (String)row.get("label"));
                    node.put("description",
                             getString (row.get("description"), 0));
                    node.put("category", "Inheritance");
                    //logger.info("########### "+node);
                    hpmap.put(curie, node);
                    return true;
                }, HP_INHERITANCE);
            logger.info("####### "+hpmap.size()+" HPO inheritance loaded!");
            hpInheritance.put("UMLS:C0441748", hpmap.get("HP:0000007"));
            hpInheritance.put("Orphanet:409930", hpmap.get("HP:0000007"));
            hpInheritance.put("UMLS:C0443147", hpmap.get("HP:0000006"));
            hpInheritance.put("Orphanet:409929", hpmap.get("HP:0000006"));
            hpInheritance.put("UMLS:C0241764", hpmap.get("HP:0001417"));
            hpInheritance.put("UMLS:C4538796", hpmap.get("HP:0001417"));
            hpInheritance.put("UMLS:C1845977", hpmap.get("HP:0001419"));
            hpInheritance.put("Orphanet:409932", hpmap.get("HP:0001419"));
            hpInheritance.put("UMLS:C1847879", hpmap.get("HP:0001423"));
            hpInheritance.put("Orphanet:409934", hpmap.get("HP:0001423"));
            hpInheritance.put("UMLS:C0887941", hpmap.get("HP:0001427"));
            hpInheritance.put("Orphanet:409933", hpmap.get("HP:0001427"));
            hpInheritance.put("UMLS:C2748900", hpmap.get("HP:0001450"));
            hpInheritance.put("Orphanet:409938", hpmap.get("HP:0001450"));
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE,
                       "Can't retrieve HPO inheritance data!", ex);
        }
    }

    void setupSpecialties () {
        try {
            specialties.put("Genetics", new Specialty[]{
                    new Specialty (ef, "GARD:S001", "S_MONDO", "MONDO:0003847")
                });
            specialties.put("Chromosomal Anomaly", new Specialty[]{
                    new Specialty (ef, "GARD:S002", "S_MONDO", "MONDO:0019040")
                });
            specialties.put("Cancer", new Specialty[]{
                    new Specialty (ef, "GARD:S003", "S_MONDO", "MONDO:0023370")
                });
            specialties.put("Autoimmune", new Specialty[]{
                    new Specialty (ef, "GARD:S004", "S_MONDO", "MONDO:0007179",
                                    "MONDO:0019751")
                });
            specialties.put("Cardiology", new Specialty[]{
                    new Specialty (ef, "GARD:S005", "S_MONDO", "MONDO:0005267")
                });
            specialties.put("Dermatology", new Specialty[]{
                    new Specialty (ef, "GARD:S006", "S_MONDO", "MONDO:0005093")
                });
            specialties.put("Endocrine", new Specialty[]{
                    new Specialty (ef, "GARD:S007", "S_MONDO", "MONDO:0005151")
                });
            specialties.put("Gastroenterology", new Specialty[]{
                    new Specialty (ef, "GARD:S008", "S_MONDO", "MONDO:0004335")
                });
            specialties.put("Hematology", new Specialty[]{
                    new Specialty (ef, "GARD:S009", "S_MONDO", "MONDO:0005570")
                });
            specialties.put("Immunology", new Specialty[]{
                    new Specialty (ef, "GARD:S010", "S_MONDO", "MONDO:0005046")
                });
            specialties.put("Infectious Disease", new Specialty[]{
                    new Specialty (ef, "GARD:S011", "S_MONDO", "MONDO:0005550")
                });
            specialties.put("Nephrology", new Specialty[]{
                    new Specialty (ef, "GARD:S012", "S_MONDO", "MONDO:0005240")
                });
            specialties.put("Neurology", new Specialty[]{
                    new Specialty (ef, "GARD:S013", "S_MONDO", "MONDO:0005071"),
                    new Specialty (ef, "GARD:S013", "S_HP", "HP:0001298",
                                   "HP:0012759", "HP:0001250")
                });
            specialties.put("Oncology", new Specialty[]{
                    new Specialty (ef, "GARD:S014", "S_MONDO", "MONDO:0023370"),
                    new Specialty (ef, "GARD:S014", "S_HP", "HP:0002664")
                });
            specialties.put("Ophthalmology", new Specialty[]{
                    new Specialty (ef, "GARD:S015", "S_MONDO", "MONDO:0024458")
                });
            specialties.put("Psychiatry", new Specialty[]{
                    new Specialty (ef, "GARD:S016", "S_MONDO", "MONDO:0002025"),
                    new Specialty (ef, "GARD:S016", "S_HP", "HP:0000729",
                                   "HP:0100851", "HP:0031466")
                });
            specialties.put("Pulmonology", new Specialty[]{
                    new Specialty (ef, "GARD:S017", "S_MONDO", "MONDO:0005087")
                });
            specialties.put("Rheumatology", new Specialty[]{
                    new Specialty (ef, "GARD:S018", "S_MONDO", "MONDO:0005554")
                });
            specialties.put("Vascular Medicine", new Specialty[]{
                    new Specialty (ef, "GARD:S019", "S_MONDO",  "MONDO:0018882")
                });
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "specialties failed", ex);
        }
    }

    void downloadGHRUrls () {
        try {
            logger.info("## downloading GHR gene mappings...");
            URL url = new URL
                ("https://medlineplus.gov/download/ghr-summaries.xml");
            DocumentBuilder builder = DocumentBuilderFactory
                .newInstance().newDocumentBuilder();
            Element root = builder.parse(url.openStream()).getDocumentElement();
            XPath xpath = XPathFactory.newInstance().newXPath();
            NodeList nodes = (NodeList) xpath.evaluate
                ("//summaries/health-condition-summary/"
                 +"related-gene-list/related-gene",
                 root, XPathConstants.NODESET);
            for (int i = 0; i < nodes.getLength(); ++i) {
                Element n = (Element)nodes.item(i);
                String gene = xpath.evaluate("./gene-symbol", n);
                String page = xpath.evaluate("./ghr-page", n);
                //logger.info("..."+gene+" => "+page);
                ghrUrls.put(gene, page);
            }
            logger.info("## "+ghrUrls.size()+" GHR gene URLs mapped!");
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't download GHR urls!", ex);
        }
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

    HP_Type[] getHpTypes (String id) {
        HP_Type[] types = hpTypes.get(id);
        if (types == null) {
            Set<HP_Type> alltypes = EnumSet.allOf(HP_Type.class);
            Set<HP_Type> found = new HashSet<>();
            done: for (HP_Type t : alltypes) {
                switch (t) {
                case Inheritance:
                case Onset:
                case Mortality:
                case Progression:
                    if (t.isType(ef, id)) {
                        found.add(t);
                        break done;
                    }
                    // only one of the types is allowed for these
                    break;

                case Symptom: // do nothing
                    break;
                    
                default:
                    if (t.isType(ef, id))
                        found.add(t);
                }
            }

            if (found.isEmpty()) {
                types = new HP_Type[]{ HP_Type.Symptom };
            }
            else {
                if (found.size() > 1) {
                    found.remove(HP_Type.Lab);
                }
                types = found.toArray(new HP_Type[0]);
            }
            hpTypes.put(id, types);
        }
        return types;
    }

    String getHpTypesAsString (String id) {
        StringBuilder sb = new StringBuilder ();
        HP_Type[] types = getHpTypes (id);
        if (types != null) {
            for (HP_Type t : types) {
                if (sb.length() > 0) sb.append(";");
                sb.append(t);
            }
        }
        return sb.toString();
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
                Source src1 = getSource (source);
                Source src2 = getSource (target);
                if (src1 != null && src2 != null) {
                    logger.info(src1.curie(source)+" ["+src1.name+":"
                                +source.getId()+"] "
                                +" <=> "+src2.curie(target)+" ["
                                +src2.name+":"+target.getId()+"]..."
                                +Util.toString(values));
                }
                else if (src1 != null) {
                    logger.warning("** Node "+target.getId()
                                   +" is not a known source: "
                                   + target.labels());
                }
                else { // src2 != null
                    logger.warning("** Node "+source.getId()
                                   +" is not a known source: "
                                   + source.labels());
                }
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

    public int writeJson (String base) throws Exception {
        try (PrintStream diseases = new PrintStream (new FileOutputStream
                                                     (base+"_diseases.json"));
             PrintStream genes = new PrintStream (new FileOutputStream
                                                  (base+"_genes.json"));
             PrintStream allgenes = new PrintStream (new FileOutputStream
                                                     (base+"_allgenes.json"));
             PrintStream allpheno = new PrintStream (new FileOutputStream
                                                     (base+"_allpheno.json"));
             PrintStream phenotypes = new PrintStream
             (new FileOutputStream (base+"_phenotypes.json"));
             PrintStream drugs = new PrintStream (new FileOutputStream
                                                  (base+"_drugs.json"))) {
            allgenes.print("[");
            allpheno.print("[");
            OMIMGenes omim = new OMIMGenes (allgenes);
            HPOPhenotypes pheno = new HPOPhenotypes (allpheno);
            allgenes.println("]\n}]");
            allpheno.println("]\n}]");
            
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
                          PrintStream dps) throws Exception {
        ef.stitches((source, target, values) -> {
                /*
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                */
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        // now merge S_GHR based on its mapping to S_MEDLINEPLUS
        // since it neither has R_exactMatch nor R_equivalentClass
        ef.entities("S_GHR", e -> {
                Set<Long> clz = new HashSet<>();
                e.neighbors((id, xe, key, reversed, props) -> {
                        if (xe.is("S_MEDLINEPLUS")) {
                            Long c = uf.root(xe.getId());
                            if (c != null)
                                clz.add(c);
                        }
                        return true;
                    }, I_CODE);
                
                if (clz.size() > 1) {
                    logger.warning("GHR node \""+getString (e.payload("id"))
                                   +"\" maps to "+clz.size()
                                   +" components: "+clz);
                }
                else if (clz.size() == 1) {
                    uf.union(e.getId(), clz.iterator().next());
                }
            });

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
            DiseaseComponent dc = new DiseaseComponent (gardOnt, comp);
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

        //FIXME: add three new diseases here
        ps.println("]");

        logger.info("######## "+components.length+" diseases!");
        logger.info("######## "+genes.size()+" genes!");
        logger.info("######## "+phenotypes.size()+" phenotypes!");
        logger.info("######## "+drugs.size()+" drugs!");

        try (FileWriter fw = new FileWriter ("gard.owl")) {
            gardOnt.write(fw);
        }
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
            name = getString (e.payload("OGG_0000000005"), 0);
        }
        else if (e.is("S_ORDO_ORPHANET")) {
            sym = getString (e.payload("symbol"));
            name = getString (e.payload("label"), 0);
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
        String url = ghrUrls.get(sym);
        if (url == null) {
            logger.warning("** No GHR url for gene "+sym
                           +"; resort to source!");
            url = getString (e.payload("uri"), 0);
        }
        ps.println("        \"GHR_URL__c\": \""+url+"\",");
        ps.println("        \"Gene_Type__c\": \""+type+"\",");        
        ps.println("        \"Chromosome_Location__c\": \""+locus+"\",");
        ps.println("        \"Gene_Identifier__c\": \""+id+"\"");
        ps.print  ("    }");

        gardOnt.createGene(id, "http://identifiers.org/"
                           +id.toLowerCase().replaceAll(":", "/"))
            .addLabel(sym)
            .addDescription(name)
            ;
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
                   +getString (e.payload("label"), 0)+"\",");
        ps.println("        \"HPO_Synonym__c\": \""
                   +getString (e.payload("hasExactSynonym"), 0)+"\",");
        String hpid = (String) e.payload("notation");
        String cat = getHpCategory (hpid);
        if (cat == CATEGORY_NONE) {
            logger.warning(hpid+": can't find phenotype category!");
        }
        ps.println("        \"HPO_Category__c\": \""+cat+"\",");
        ps.println("        \"HPO_Description__c\": \""+
                   getString (e.payload("IAO_0000115"), 0)+"\",");
        ps.println("        \"HPO_Feature_URL__c\": \""
                   +"https://hpo.jax.org/app/browse/term/"+hpid+"\",");
        HP_Type[] types = getHpTypes (hpid);
        if (1 == types.length && types[0] == HP_Type.Lab) {
            // if it belongs to more than one categories, then it's not a lab
            String[] cats = cat.split(";");
            if (cats.length > 1) {
                types[0] = HP_Type.Symptom;
            }
        }
        StringBuilder type = new StringBuilder ();
        for (int i = 0; i < types.length; ++i) {
            if (i > 0) type.append("; ");
            type.append(types[i]);
        }
        ps.println("        \"HPO_Feature_Type__c\": \""+type+"\",");
        ps.println("        \"External_ID__c\": \""
                   +getString (e.payload("notation"))+"\"");
        ps.print("    }");

        gardOnt.createPhenotype
            (hpid, "http://purl.obolibrary.org/obo/"+hpid.replaceAll(":", "_"))
            .addLabel(getString (e.payload("label")))
            .addDescription(getString (e.payload("IAO_0000115")))
            ;
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
                   +getString (e.payload("CompoundName"), 0)+"\",");
        ps.println("        \"Drug_Identifier__c\": \"UNII:"+unii+"\",");
        final Set<String> syns = new TreeSet<>();
        e.neighbors((id, xe, key, reversed, props) -> {
                if ("Approved".equals(props.get("HighestPhase"))
                    && "indication_of".equals(props.get("value"))
                    && xe.is(RANCHO_LABEL)) {
                    syns.add(getString (xe.payload("ConditionProductName"), 0));
                }
                return true;
            }, R_rel);
        ps.println("        \"Drug_Synonyms__c\": \""
                   +getString (syns.toArray(new String[0]), 0)+"\"");
        ps.print  ("    }");

        gardOnt.createDrug(unii, "https://drugs.ncats.io/drug/"+unii)
            .addLabel(getString (e.payload("CompoundName")))
            ;
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
