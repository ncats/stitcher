package ncats.stitcher.disease;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;

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
    static final int BATCH_SIZE = 10;

    static class Source implements Comparable {
        final String name;
        final String id;
        final String prefix;
        final String label;
        final String source;
        final String[] synonyms;

        Source (String name, String id, String prefix,
                String label, String source, String... synonyms) {
            this.name = name;
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
        SOURCES.add(new Source ("S_OMIM", "notation", "", "label",
                                "OMIM", "altLabel"));
        SOURCES.add(new Source ("S_MEDGEN", "id", "MEDGEN:", "NAME",
                                "MedGen", "SYNONYMS"));
        SOURCES.add(new Source ("S_MESH", "notation", "", "label",
                                "MeSH", "altLabel"));
        SOURCES.add(new Source ("S_DOID", "notation", "", "label",
                                "DiseaseOntology", "hasExactSynonym"));
        SOURCES.add(new Source ("S_MEDLINEPLUS", "notation", "",
                                "label", "MedlinePlus", "altLabel"));
        SOURCES.add(new Source ("S_EFO", "notation", "", "label", "EFO",
                                "hasExactSynonym", "hasRelatedSynonym"));
    }

    static final String HP_CATEGORY_FMT = "match (d:DATA)-->(n:S_HP)-"
        +"[e:R_subClassOf*0..13]->(m:S_HP)-[:R_subClassOf]->(:S_HP)--(z:DATA) "
        +"where d.notation='%1$s' and all(x in e where x.source=n.source or "
        +"n.source in x.source) and z.notation='HP:0000118' with m match "
        +"p=(m)<--(d:DATA) return distinct d.label as label order by label";

    static final String HP_INHERITANCE_FMT = "match (d:DATA)-->(n:S_HP)-[e:R_subClassOf*0..]->(:S_HP)--(z:DATA) where d.notation='%1$s' and all(x in e where x.source=n.source or n.source in x.source) and z.notation='HP:0000005' return d.label as inheritance";

    // HP:0040006 - mortality/aging
    // HP:0003674 - onset

    static final String HP_CATEGORY_NONE = "";
    
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
    
    static Source getSource (String name) {
        for (Source s : SOURCES)
            if (s.equals(name))
                return s;
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
                StringBuilder sb = new StringBuilder ((String)Array.get(value, 0));
                for (int i = 1; i < Array.getLength(value); ++i)
                    sb.append("; "+Array.get(value, i));
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
        ArrayNode genes = mapper.createArrayNode();
        Set<Entity> neighbors = new LinkedHashSet<>();
        String notation;
        
        OrphanetGenes (Entity... ents) {
            if (ents != null && ents.length > 0) {
                for (Entity e : ents) {
                    notation = getString (e.payload("notation"));
                    e.neighbors(this, R_rel);
                }
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (!reversed && xe.is("S_ORDO_ORPHANET")
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
            if ("ORPHA:79445".equals(notation) && xe.is("S_ORDO_ORPHANET")) {
                logger.info("..."+notation+" -["+relname+"]-> "+getString (xe.payload("notation"))+" reversed="+reversed+" labels="+xe.labels()+" props="+props);
            }
            if (!reversed && xe.is("S_ORDO_ORPHANET")
                && relname.equals(props.get("name"))) {
                ObjectNode node = newJsonObject ();
                node.put("curie", notation);
                node.put("label", getString (xe.payload("label")));
                neighbors.add(node);
            }
            return true;
        }
    }

    class GARDPhenotypes implements NeighborVisitor {
        ArrayNode phenotypes = mapper.createArrayNode();
        String gardId;
        Set<Entity> neighbors = new LinkedHashSet<>();
        
        GARDPhenotypes (Entity... ents) {
            if (ents != null && ents.length > 0) {
                 for (Entity e : ents) {
                    gardId = getString (e.payload("gard_id"));
                    e.neighbors(this, R_hasPhenotype);
                }
            }
        }

        public boolean visit (long id, Entity xe, StitchKey key,
                              boolean reversed, Map<String, Object> props) {
            if (!reversed && xe.is("S_HP")) {
                ObjectNode node = newJsonObject ();
                String hpid = (String) xe.payload("notation");
                node.put("curie", hpid);
                String cat = getHpCategory (hpid);
                if (cat == HP_CATEGORY_NONE) {
                    logger.warning(gardId+" -> " +hpid+" ("
                                   +xe.payload("label")
                                   +") has no category!");
                }
                node.put("category", cat);
                                
                node.put("label", getString (xe.payload("label")));
                node.put("synonym",
                         getString (xe.payload("hasExactSynonym")));
                node.put("description",
                         getString (xe.payload("IAO_0000115")));
                String freq = getString (props.get("Frequency"));
                if (freq.startsWith("HP:")) {
                    for (Iterator<Entity> iter = ef.find("notation", freq);
                         iter.hasNext(); ) {
                        Entity e = iter.next();
                        freq = getString (e.payload("label"));
                    }
                }
                node.put("frequency", freq);
                node.put("hpo_method",
                         getString (props.get("Evidence")));
                node.put("sex", getString (props.get("Sex")));
                node.put("source_curie",
                         getString (props.get("Reference")));
                node.put("modifier",
                         getString (props.get("Modifier")));
                node.put("phenotype_sfdc_id", "");
                phenotypes.add(node);
                neighbors.add(xe);
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
        
        DiseaseComponent (long[] comp) {
            component = ef.component(comp);
            // partition entities into data sources
            Map<Source, Set<Entity>> map = new TreeMap<>();
            for (Entity e : component) {
                Set<String> labels = e.labels();
                for (String lbl : labels) {
                    Source src = getSource (lbl);
                    if (src != null) {
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
        
        Entity[] get (String source) {
            return entities.get(getSource (source));
        }

        boolean has (String source) {
            return entities.containsKey(getSource (source));
        }
        
        JsonNode toJson () {
            ObjectNode disease = null;
            if (gard != null) {
                disease = mapper.createObjectNode();            
                disease.put("term", doTerm ());
                disease.put("synonyms", doSynonyms ());
                disease.put("external_identifiers", doExternalIdentifiers ());
                disease.put("inheritance", doInheritance ());
                disease.put("age_at_onset", doAgeOfOnset ());
                disease.put("age_at_death", doAgeOfDeath ());
                disease.put("epidemiology", doEpidemiology ());
                disease.put("genes", doGenes ());
                disease.put("phenotypes", doPhenotypes ());
                disease.put("evidence", doEvidence ());
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
                term.put("description_URL", getSource("S_MEDGEN").url(medgen[0]));
            }

            JsonNode desc = term.get("description");
            if (desc != null && desc.isNull() && orpha != null) {
                term.put("description", getString (orpha[0].payload("definition")));
                term.put("description_curie",
                         getString (orpha[0].payload("notation")));
                term.put("description_URL", getSource("S_ORDO_ORPHANET").url(orpha[0]));
            }

            desc = term.get("description");
            if (desc != null && desc.isNull() && mondo != null) {
                term.put("description", getString (mondo[0].payload("IAO_0000115")));
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

        JsonNode doSynonyms () {
            ArrayNode synonyms = mapper.createArrayNode();            
            for (Entity e : gard) {
                ObjectNode syn = newJsonObject ();
                syn.put("curie", getString (e.payload("gard_id")));
                syn.put("label", getString (e.payload("name")));
                synonyms.add(syn);
            }
            for (Source src : SOURCES) {
                Entity[] ents = entities.get(src);
                if (ents != null) {
                    for (Entity e : ents) {
                        String curie = src.prefix+e.payload(src.id);
                        for (String s : src.synonyms) {
                            for (Object x : Util.toArray(e.payload(s))) {
                                ObjectNode syn = newJsonObject ();
                                syn.put("curie", curie);
                                syn.put("label", x.toString());
                                synonyms.add(syn);
                            }
                        }
                    }
                }
            }
            return synonyms;
        }

        JsonNode doExternalIdentifiers () {
            ArrayNode xrefs = mapper.createArrayNode();
            for (Source src : SOURCES) {
                Entity[] ents = entities.get(src);
                if (ents != null) {
                    for (Entity e : ents) {
                        ObjectNode node = newJsonObject ();
                        node.put("curie", getString (e.payload(src.id)));
                        node.put("url", src.url(e));
                        node.put("source", src.source);
                        xrefs.add(node);
                    }
                }
            }
            return xrefs;
        }
        
        JsonNode doInheritance () {
            ArrayNode inheritance = new OrphanetRelationships
                ("has_inheritance", get ("S_ORDO_ORPHANET")).neighbors;
            
            Entity[] ents = get ("S_OMIM");
            if (ents != null) {
                for (Entity e : ents) {
                    final String notation = getString (e.payload("notation"));
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!reversed && xe.is("S_OMIM")
                                && "has_inheritance_type"
                                .equals(props.get("name"))) {
                                ObjectNode node = newJsonObject ();
                                node.put("curie", notation);
                                node.put("label",
                                         getString (xe.payload("label")));
                                inheritance.add(node);
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

        JsonNode doEpidemiology () {
            return new OrphanetEpidemiology(get ("S_ORDO_ORPHANET")).epi;
        }

        JsonNode doGenes () {
            OrphanetGenes og = new OrphanetGenes(get("S_ORDO_ORPHANET"));
            this.genes.addAll(og.neighbors);
            return og.genes;
        }
            
        JsonNode doPhenotypes () {
            // TODO: other sources..
            GARDPhenotypes gp = new GARDPhenotypes (get ("S_GARD"));
            this.phenotypes.addAll(gp.neighbors);
            return gp.phenotypes;
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
                                    node.put("url", "https://www.ncbi.nlm.nih.gov/books/"
                                             +bookid);
                                    node.put("type", "Systematic Review");
                                    evidence.add(node);
                                }
                                else {
                                    logger.warning("Can't lookup GeneReviews "+gr);
                                }
                                dups.add(xe);
                            }
                            return true;
                        }, N_Name);
                }
            }
            return evidence;
        }
    }

    public HoofBeats (EntityFactory ef, int version) {
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
            final String cypher = String.format(HP_CATEGORY_FMT, id);
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
                cat = HP_CATEGORY_NONE;
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

    static String getIds (Component component, String source, String prop) {
        StringBuilder sb = new StringBuilder ();
        Entity[] entities = filter (component, source);
        if (entities.length > 0) {
            sb.append(entities[0].payload(prop));
            for (int i = 1; i < entities.length; ++i)
                sb.append(","+entities[i].payload(prop));
        }
        return sb.toString();
    }

    static Entity[] filter (Component component, String source) {
        return component.stream()
            .filter(e -> e.is(source)).toArray(Entity[]::new);
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
        
        ps.print(getIds (component, "S_MONDO", "notation")+"\t");
        ps.print(getIds (component, "S_ORDO_ORPHANET", "notation")+"\t");
        ps.print(getIds (component, "S_GARD", "gard_id")+"\t");
        ps.print(getIds (component, "S_OMIM", "notation")+"\t");
        ps.print(getIds (component, "S_MEDGEN", "id")+"\t");
        ps.print(getIds (component, "S_MESH", "notation")+"\t");
        ps.print(getIds (component, "S_DOID", "notation")+"\t");
        ps.print(getIds (component, "S_MEDLINEPLUS", "notation")+"\t");
        ps.print(getIds (component, "S_EFO", "notation"));
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
                   +"MedLinePlus\tEFO");
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
             PrintStream phenotypes = new PrintStream (new FileOutputStream
                                                       (base+"_phenotypes.json"))) {
            return writeJson (diseases, genes, phenotypes);
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
    
    public int writeJson (PrintStream dps, PrintStream gps, PrintStream pps)
        throws IOException {
        ef.stitches((source, target, values) -> {
                /*
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                */
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        int count = 0;
        Random rand = new Random ();
        Set<Long> genes = new HashSet<>();
        Set<Long> phenotypes = new HashSet<>();
        
        dps.println("[");
        gps.print("[");
        pps.print("[");
        ObjectMapper mapper = new ObjectMapper ();            
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        
        long[][] components = uf.components();
        logger.info("###### "+components.length+" components! ########");
        for (long[] comp : components) {
            DiseaseComponent dc = new DiseaseComponent (comp);
            JsonNode json = dc.toJson();
            if (json != null) {
                if (count > 0) dps.print(",");
                String jstr = writer.writeValueAsString(json);
                dps.print(jstr);
                /*
                if (count < 10 && rand.nextFloat() > 0.5f) {
                    String id = json.get("term").get("curie")
                        .asText().replaceAll(":", "_");
                    try (FileOutputStream fos =
                         new FileOutputStream (id+".json")) {
                        writer.writeValue(fos, json);
                        writeGenes (id, dc.genes);
                        writePhenotypes (id, dc.phenotypes);
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    ++count;
                }
                */
                ++count;
            }

            for (Entity e : dc.genes) {
                if (!genes.contains(e.getId())) {
                    if (genes.size() % BATCH_SIZE != 0) 
                        gps.println(",");
                    else {
                        if (!genes.isEmpty())
                            gps.println("]\n},");
                        sfJsonHeader (gps);
                    }
                    writeGene (gps, e);
                    genes.add(e.getId());
                }
            }
            
            for (Entity e : dc.phenotypes) {
                if (!phenotypes.contains(e.getId())) {
                    if (phenotypes.size() % BATCH_SIZE != 0)
                        pps.println(",");
                    else {
                        if (!phenotypes.isEmpty())
                            pps.println("]\n},");
                        sfJsonHeader (pps);
                    }
                    writePhenotype (pps, e);
                    phenotypes.add(e.getId());
                }
            }
        }
        dps.println("]");
        gps.println("]\n}]");
        pps.println("]\n}]");

        logger.info("######## "+components.length+" diseases!");
        logger.info("######## "+genes.size()+" genes!");
        logger.info("######## "+phenotypes.size()+" phenotypes!");
        
        return count;
    }

    void writeGenes (String id, Entity[] genes) throws IOException {
        try (FileOutputStream fos = new FileOutputStream (id+"_genes.json")) {
            PrintStream ps = new PrintStream (fos);
            ps.println("{");
            ps.println("  \"allOrNone\": false,"); // ?
            ps.println("  \"records\": [");
            for (int i = 0; i < genes.length; ++i) {
                Entity e = genes[i];
                if (i > 0) {
                    ps.println(",");
                }
                writeGene (ps, e);
            }
            ps.println("]");
            ps.println("}");
        }
    }

    void writeGene (PrintStream ps, Entity e) {
        ps.println("    {");
        ps.println("        \"attributes\": {");
        ps.println("            \"type\": \"Gene__c\"");
        ps.println("         },");
        ps.println("        \"Name\": \""
                   +getString (e.payload("symbol"))+"\",");
        ps.println("        \"Gene_Name__c\": \""
                   +getString (e.payload("label"))+"\",");
        ps.println("        \"GHR_URL__c\": \"\",");
        StringBuilder type = new StringBuilder ();
        StringBuilder locus = new StringBuilder ();
        e.neighbors((id, xe, key, reversed, props) -> {
                if ("hasDbXref".equals(props.get("name"))
                    && props.get("value").toString().startsWith("HGNC:")
                    && xe.is("S_OGG")) {
                    locus.append(getString (xe.payload("OGG_0000000008")));
                    type.append(getString (xe.payload("OGG_0000000018")));
                }
                return true;
            }, I_CODE);
        ps.println("        \"Gene_Type__c\": \""+type+"\",");        
        ps.println("        \"Chromosome_Location__c\": \""+locus+"\",");
        Object[] xrefs = Util.toArray(e.payload("hasDbXref"));
        boolean hasId = false;
        for (Object ref : xrefs) {
            String syn = (String)ref;
            if (syn.startsWith("HGNC:")) {
                ps.println("        \"Gene_Identifier__c\": \""
                           +syn+"\"");
                hasId = true;
            }
        }
        if (!hasId) {
            ps.println("        \"Gene_Identifier__c\": \""
                       +getString(e.payload("notation"))+"\"");
        }
        ps.print("    }");
    }

    void writePhenotypes (String id, Entity[] phenotypes) throws IOException {
        try (FileOutputStream fos = new FileOutputStream (id+"_phenotypes.json")) {
            PrintStream ps = new PrintStream (fos);
            ps.println("{");
            ps.println("  \"allOrNone\": false,"); // ?
            ps.println("  \"records\": [");
            for (int i = 0; i < phenotypes.length; ++i) {
                Entity e = phenotypes[i];
                if (i > 0) {
                    ps.println(",");
                }
                writePhenotype (ps, e);
            }
            ps.println("]");
            ps.println("}");
        }
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
        if (cat == HP_CATEGORY_NONE) {
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
            logger.info("Usage: "+HoofBeats.class.getName()+" DBDIR [VERSION]");
            System.exit(1);
        }

        int version = 1;
        if (argv.length > 1)
            version = Integer.parseInt(argv[1]);

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            HoofBeats hb = new HoofBeats (ef, version);
            //hb.beats("zebra_beats.txt");
            int pos = argv[0].indexOf('.');
            if (pos < 0)
                pos = argv[0].length();
            String base = argv[0].substring(0, pos);
            hb.writeJson(base);
        }
    }
}
