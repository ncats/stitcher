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
        +"[e:R_subClassOf*0..]->(m:S_HP)-[:R_subClassOf]->(:S_HP)--(z:DATA) "
        +"where d.notation='%1$s' and all(x in e where x.source=n.source or "
        +"n.source in x.source) and z.notation='HP:0000118' with m match "
        +"p=(m)<--(d:DATA) return distinct d.label as label";
    
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

    static String getString (Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
                StringBuilder sb = new StringBuilder ((String)Array.get(value, 0));
                for (int i = 1; i < Array.getLength(value); ++i)
                    sb.append(";"+Array.get(value, i));
                value = sb.toString();
            }
            return (String)value;
        }
        return "";
    }
    
    final EntityFactory ef;    
    final UnionFind uf = new UnionFind ();
    final ObjectMapper mapper = new ObjectMapper ();
    
    class DiseaseComponent {
        final Component component;
        final Map<Source, Entity[]> entities = new TreeMap<>();
        final Entity[] gard;
        
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

        ObjectNode newJsonObject () {
            ObjectNode obj = mapper.createObjectNode();
            obj.put("Id", "");
            return obj;
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

        void orphanetRelationships (String relname,
                                    Consumer<JsonNode> consumer) {
            Entity[] ents = get("S_ORDO_ORPHANET");
            if (ents != null) {
                Set<Entity> dups = new HashSet<>();
                for (Entity e : ents) {
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!reversed && !dups.contains(xe)
                                && xe.is("S_ORDO_ORPHANET")
                                && relname.equals(props.get("name"))) {
                                ObjectNode node = newJsonObject ();
                                node.put("curie", getString (xe.payload("notation")));
                                node.put("label", getString (xe.payload("label")));
                                dups.add(xe);
                                consumer.accept(node);
                            }
                            return true;
                        }, R_rel);
                }
            }
        }
        
        JsonNode doInheritance () {
            ArrayNode inheritance = mapper.createArrayNode();
            orphanetRelationships ("has_inheritance", n -> inheritance.add(n));
            // TODO: check for others
            return inheritance;
        }

        JsonNode doAgeOfOnset () {
            ArrayNode ageOfOnset = mapper.createArrayNode();
            orphanetRelationships ("has_age_of_onset", n -> ageOfOnset.add(n));
            return ageOfOnset;
        }

        JsonNode doAgeOfDeath () {
            ArrayNode ageOfDeath = mapper.createArrayNode();
            orphanetRelationships ("has_age_of_death", n -> ageOfDeath.add(n));
            return ageOfDeath;
        }

        JsonNode doEpidemiology () {
            ArrayNode epidemiology = mapper.createArrayNode();
            Entity[] ents = get ("S_ORDO_ORPHANET");
            if (ents != null) {
                for (Entity e : ents) {
                    e.data(props -> {
                            ObjectNode n = newJsonObject ();
                            n.put("class", (String)props.get("PrevalenceClass"));
                            n.put("geographic", (String)props.get("PrevalenceGeographic"));
                            n.put("qualification",
                                  (String)props.get("PrevalenceQualification"));
                            n.put("type", (String)props.get("PrevalenceType"));
                            Object source = props.get("Source");
                            n.put("source_curie", getString (source));
                            Object valmoy = props.get("ValMoy");
                            //n.put("valmoy", (Double)valmoy);
                            n.put("valmoy", valmoy != null ? valmoy.toString():"");
                            n.put("source_validation",
                                  (String)props.get("PrevalenceValidationStatus"));
                            epidemiology.add(n);
                        }, "PREVALENCE");
                }
            }
            return epidemiology;
        }

        JsonNode doGenes () {
            ArrayNode genes = mapper.createArrayNode();
            Entity[] ents = get("S_ORDO_ORPHANET");
            if (ents != null) {
                Set<Entity> dups = new HashSet<>();
                for (Entity e : ents) {
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!reversed && !dups.contains(xe)
                                && xe.is("S_ORDO_ORPHANET")
                                && "disease_associated_with_gene"
                                .equals(props.get("name"))) {
                                ObjectNode node = newJsonObject ();
                                Object[] xrefs =
                                    Util.toArray(xe.payload("hasDbXref"));
                                for (Object ref : xrefs) {
                                    String syn = (String)ref;
                                    if (syn.startsWith("HGNC:"))
                                        node.put("curie", syn);
                                }
                                node.put("source_curie",
                                         getString (xe.payload("notation")));
                                node.put("source_label",
                                         getString (xe.payload("label")));
                                node.put("gene_symbol",
                                         getString (xe.payload("symbol")));
                                node.put("association_type",
                                         getString (props.get
                                                    ("DisorderGeneAssociationType")));
                                node.put("source_validation", getString
                                         (props.get
                                          ("DisorderGeneAssociationValidationStatus")));
                                node.put("gene_sfdc_id", "");
                                dups.add(xe);
                                genes.add(node);
                            }
                            return true;
                        }, R_rel);
                }
            }
            
            return genes;
        }

        JsonNode doPhenotypes () {
            ArrayNode phenotypes = mapper.createArrayNode();
            Entity[] ents = get("S_GARD");
            if (ents != null) {
                Set<Entity> dups = new HashSet<>();
                for (Entity e : ents) {
                    e.neighbors((id, xe, key, reversed, props) -> {
                            if (!reversed
                                && !dups.contains(xe) && xe.is("S_HP")) {
                                ObjectNode node = newJsonObject ();
                                String hpid = (String) xe.payload("notation");
                                node.put("curie", hpid);
                                ef.cypher(row -> {
                                        Object label = row.get("label");
                                        StringBuilder sb = new StringBuilder ();
                                        for (Object obj : Util.toArray(label)) {
                                            String cat = (String)obj;
                                            if (sb.length() > 0) sb.append(";");
                                            sb.append(cat.replaceAll("Abnormality of the", "")
                                                      .replaceAll("Abnormality of", "")
                                                      .replaceAll("abnormality", "")
                                                      .replaceAll("Abnormal", "").trim());
                                        }
                                        
                                        if (sb.length() > 0) {
                                            node.put("category", sb.toString());
                                        }
                                        return false;
                                    }, String.format(HP_CATEGORY_FMT, hpid));
                                node.put("label", getString (xe.payload("label")));
                                node.put("synonym",
                                         getString (xe.payload("hasExactSynonym")));
                                node.put("description",
                                         getString (xe.payload("IAO_0000115")));
                                node.put("frequency",
                                         getString (props.get("Frequency")));
                                node.put("hpo_method",
                                         getString (props.get("Evidence")));
                                node.put("sex", getString (props.get("Sex")));
                                node.put("source_curie",
                                         getString (props.get("Reference")));
                                node.put("modifier",
                                         getString (props.get("Modifier")));
                                node.put("phenotype_sfdc_id", "");
                                dups.add(xe);
                                phenotypes.add(node);
                            }
                            return true;
                        }, R_hasPhenotype);
                }
            }
            
            // TODO: other sources..
            
            return phenotypes;
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
                                    node.put("label", getString (xe.payload("title")));
                                    node.put("genes", toJsonArray (xe.payload("genes")));
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

    public int writeJson (String outfile) throws IOException {
        return writeJson (new FileOutputStream (outfile));
    }
    
    public int writeJson (OutputStream os) throws IOException {
        ef.stitches((source, target, values) -> {
                /*
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                */
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        int count = 0;        
        PrintStream ps = new PrintStream (os);
        ps.print("[");
        ObjectMapper mapper = new ObjectMapper ();            
        ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();
        long[][] components = uf.components();
        Random rand = new Random ();
        for (long[] comp : components) {
            DiseaseComponent dc = new DiseaseComponent (comp);
            JsonNode json = dc.toJson();
            if (json != null) {
                if (count > 0) ps.print(",");
                String jstr = writer.writeValueAsString(json);
                ps.print(jstr);
                if (count < 10 && rand.nextFloat() > 0.5f) {
                    String id = json.get("term").get("curie")
                        .asText().replaceAll(":", "_");
                    try (FileOutputStream fos =
                         new FileOutputStream (id+".json")) {
                        writer.writeValue(fos, json);
                    }
                    catch (IOException ex) {
                        ex.printStackTrace();
                    }
                    ++count;
                }
            }
        }
        if (count > 0)
            ps.println("]");
        return count;
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
            hb.writeJson("hoofbeats.json");
        }
    }
}
