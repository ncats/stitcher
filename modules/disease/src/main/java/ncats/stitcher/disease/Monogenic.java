package ncats.stitcher.disease;

import java.io.*;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.util.stream.Collectors;
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

public class Monogenic {
    static final Logger logger = Logger.getLogger(Monogenic.class.getName());

    final static String MONOGENIC_QUERY =
        "match p=(d)-[:PAYLOAD]->(n:S_OMIM)-[:I_CODE]-(:`S_MONOGENIC DISEASE`)-[e:I_GENE]-(m:S_ORDO_ORPHANET)<-[:PAYLOAD]-(z) where d.MIMTYPE='3' and (n)-[:I_GENE]-(m) "
        +"optional match (m)-[:I_CODE]-(g:S_OGG) "
        +"optional match (m)-[:I_GENE]-(g2:S_OGG)<-[:PAYLOAD]-(ogg) "
        +" where ogg.label=z.symbol "
        +"optional match q=(n)-[:R_equivalentClass|:R_exactMatch]-(:S_MONDO)-[:R_equivalentClass|:R_exactMatch]-(o:S_ORDO_ORPHANET)-[a:R_rel{name:'disease_associated_with_gene'}]->(m) "
        +"optional match r=(x)-[:PAYLOAD]->(o) "
        +"return distinct d.notation as OMIM_ID, d.label as OMIM_Disease, d.GENELOCUS as OMIM_Locus, x.notation as Orpha_ID, x.label as Orpha_Disease,e.value as Gene, id(g) as OGG1, id(g2) as OGG2, a.DisorderGeneAssociationType as Type,a.DisorderGeneAssociationValidationStatus as Status order by Gene, OMIM_ID";

    final static String MONOGENIC_QUERY2 =
        "match p=(d)-[:PAYLOAD]->(m:S_ORDO_ORPHANET)"
        +"<-[e:R_rel{name:'disease_associated_with_gene'}]"
        +"-(n:S_ORDO_ORPHANET)<-[:PAYLOAD]-(z) "
        +"match (m)-[:I_GENE]-(r:S_TCRD660)<-[:PAYLOAD]-(t) "
        +"match (n)-[:I_CODE]-(w:`S_MONOGENIC DISEASE`)<-[:PAYLOAD]-(x) "
        +"where x.HGNC = d.symbol "
        +"match (w)-[:I_CODE]-(:S_OMIM)<-[:PAYLOAD]-(y) where y.MIMTYPE='3' "
        +"optional match (r)-[:I_GENE]-(:S_OGG)<-[:PAYLOAD]-(g) "
        +"where g.label=d.symbol "
        +"return distinct t.tdl as TDL,d.symbol as Gene, "
        +"g.OGG_0000000008 as OGG_Locus, "
        +"e.DisorderGeneAssociationType as Disease_Gene_Association_Type, "
        +"z.notation as Orphanet_ID, z.label as Orphanet_Disease, "
        +"y.notation as OMIM_ID,y.label as OMIM_Disease order by d.symbol "
        //+"limit 20"
        ;

    final static String DISEASE_QUERY =
        "match (d)-[:PAYLOAD]->(o:S_OMIM) where d.notation = '%1$s' "
        +"optional match (o)-[:R_exactMatch|:R_equivalentClass]-(n:S_MONDO)<-[:PAYLOAD]-(m) "
        +"optional match (x)-[:PAYLOAD]->(:S_ORDO_ORPHANET)-[:R_equivalentClass|:R_exactMatch]-(n) "
        +"optional match (y)-[:PAYLOAD]->(:S_GARD)-[:R_exactMatch|:R_equivalentClass]-(n) "
        +"optional match (z)-[:PAYLOAD]->(:S_DOID)-[:R_exactMatch|:R_equivalentClass]-(n) return distinct d.label as OMIM_Disease,d.MIMTYPEMEANING as OMIM_Type, m.notation as MONDO_ID, m.label as MONDO_Disease, x.notation as Orphanet_ID, x.label as Orphanet_Disease, y.gard_id as GARD_ID, y.name as GARD_Disease, z.notation as DO_ID, z.label as DO_Disease";

    class Paths {
        final ObjectNode vocab = mapper.createObjectNode();
        final ObjectNode paths = mapper.createObjectNode();
        
        final Map<String, Entity> dict = new HashMap<>();
        final String source;
    
        Paths (String source) {
            this.source = source;
        }

        public ObjectNode addPathsIfAbsent (String curie) {
            return addPathsIfAbsent (this.source, curie);
        }
        
        public ObjectNode addPathsIfAbsent (String source, String curie) {
            ObjectNode retnode;            
            if (!paths.has(curie)) {
                final ObjectNode node = vocab.has(curie)
                    ? (ObjectNode)vocab.get(curie) : mapper.createObjectNode();
                final ArrayNode pa = mapper.createArrayNode();
                ef.cypher(row -> {
                        Long id = (Long)row.get("id");
                        for (Entity[] parents : ef.parents(id, R_subClassOf)) {
                            Util.reverse(parents);
                            if (node.size() == 0) {
                                add (parents[0], node);
                            }
                            ArrayNode pn = mapper.createArrayNode();
                            for (Entity e : parents) {
                                String _id = add (e);
                                pn.add(_id);
                            }
                            pa.add(pn);
                        }
                        return true;
                    }, "match (d)-[:PAYLOAD]->(n:`"+source+"`) "
                    +"where d.notation='"+curie+"' return id(n) as id");
                if (pa.size() == 0) {
                    logger.warning(curie+" has no ancestor paths!");
                }
                paths.put(curie, pa);
                retnode = node;
            }
            else {
                retnode = (ObjectNode)vocab.get(curie);
            }
            return retnode;
        }

        String add (Entity e) {
            return add (e, null);
        }
        String add (Entity e, ObjectNode node) {
            String curie = (String)e.payload("notation");
            if (!dict.containsKey(curie) || dict.get(curie).is("TRANSIENT")) {
                if (node == null)
                    node = mapper.createObjectNode();
                ObjectNode n = instrument (e, node);
                //debug("+++ instrumenting node for "+curie, n);
                vocab.put(curie, n);
                dict.put(curie, e);
            }
            return curie;
        }
        
        ObjectNode instrument (Entity e, ObjectNode n) {
            return n;
        }
    } // Paths

    class HPPaths extends Paths {
        HPPaths () {
            super ("S_HP");
        }
        
        @Override
        ObjectNode instrument (Entity e, ObjectNode n) {
            set (n, "label", e.payload("label"));
            set (n, "description", e.payload("IAO_0000115"));
            set (n, "synonyms", e.payload("hasExactSynonym"));
            set (n, "xrefs", e.payload("hasDbXref"));
            n.put("diseases", mapper.createObjectNode());
            return n;
        }
    }

    class GOPaths extends Paths {
        GOPaths () {
            super ("S_GO");
        }

        @Override
        ObjectNode instrument (Entity e, ObjectNode n) {
            set (n, "label", e.payload("label"));
            set (n, "description", e.payload("IAO_0000115"));
            set (n, "type", e.payload("hasOBONamespace"));
            n.put("genes", mapper.createObjectNode());
            return n;
        }
    }
    
    final EntityFactory ef;
    final ObjectMapper mapper = new ObjectMapper ();
    final ObjectNode genes = mapper.createObjectNode();
    final ObjectNode diseases = mapper.createObjectNode();
    final HPPaths hp = new HPPaths ();
    final GOPaths go = new GOPaths ();
    
    public Monogenic (EntityFactory ef) {
        this.ef = ef;
    }

    static String getString (Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                return (String)Array.get(value, 0);
            }
        }
        return (String)value;
    }

    static ObjectNode set (ObjectNode n, String field, Object value) {
        if (value != null) {
            n.put(field, getString (value));
        }
        return n;
    }

    void debug (String mesg, JsonNode node) {
        try {
            logger.info(mesg+"\n"+mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(node));
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    ArrayNode getArrayNode (Object value) {
        return getArrayNode (value, null);
    }
    ArrayNode getArrayNode (Object value, ArrayNode n) {
        if (n == null)
            n = mapper.createArrayNode();
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i)
                    n.add((String)Array.get(value, i));
            }
            else {
                n.add(value.toString());
            }
        }
        return n;
    }

    ObjectNode createGeneIfAbsent (String value, Consumer<ObjectNode> postfn) {
        ObjectNode node = (ObjectNode)genes.get(value);
        if (node == null) {
            node = mapper.createObjectNode();
            if (postfn != null)
                postfn.accept(node);
            genes.put(value, node);
        }
        return node;
    }

    ObjectNode instrumentGene (Entity e, ObjectNode n) {
        ArrayNode terms = getArrayNode (e.payload("IAO_0000118"));
        final String sym = getString (e.payload("OGG_0000000004"));
        terms.insert(0, sym);
        n.put("terms", terms);
        set (n, "locus", e.payload("OGG_0000000008"));
        set (n, "type", e.payload("OGG_0000000018"));
        set (n, "description", e.payload("description"));
        set (n, "comment", e.payload("comment"));
        n.put("xrefs", getArrayNode (e.payload("hasDbXref")));
        final ObjectNode annotations = mapper.createObjectNode();
        ef.cypher(row -> {
                String goid = getString (row.get("id"));
                ObjectNode ag = mapper.createObjectNode();
                set (ag, "label", row.get("label"));
                set (ag, "type", row.get("ns"));
                annotations.put(goid, ag);

                ObjectNode node = go.addPathsIfAbsent(goid);
                ObjectNode ga = mapper.createObjectNode();
                set (ga, "locus", n.get("locus").asText());
                set (ga, "type", n.get("type").asText());
                ((ObjectNode)node.get("genes")).put(sym, ga);
                return true;
            }, "match (n:S_OGG)-"
            +"[:R_rel{name:'has_go_association'}]->(:S_GO)"
            +"<-[:PAYLOAD]-(d) where id(n) = "+e.getId()
            +" and not exists(d.deprecated) "
            +"return d.notation as id, "
            +"d.hasOBONamespace as ns, d.label as label");
        n.put("go_annotations", annotations);
        n.put("diseases", mapper.createObjectNode());
        return n;
    }

    ObjectNode createDiseaseIfAbsent (String value) {
        return createDiseaseIfAbsent (value, null);
    }
    
    ObjectNode createDiseaseIfAbsent (final String value,
                                      Consumer<ObjectNode> postfn) {
        ObjectNode node = (ObjectNode)diseases.get(value);
        if (node == null) {
            final ObjectNode n = mapper.createObjectNode();
            n.put("curie", value);
            ef.cypher(row -> {
                    n.put("label", getString (row.get("OMIM_Disease")));
                    n.put("type", getString (row.get("OMIM_Type")));
                    
                    ArrayNode related = mapper.createArrayNode();
                    n.put("related", related);
                    
                    ObjectNode d = mapper.createObjectNode();
                    d.put("curie", getString (row.get("MONDO_ID")));
                    d.put("label", getString (row.get("MONDO_Disease")));
                    related.add(d);
                    
                    if (null != row.get("Orphanet_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (row.get("Orphanet_ID")));
                        d.put("label", getString (row.get("Orphanet_Disease")));
                        related.add(d);
                    }
                    if (null != row.get("GARD_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (row.get("GARD_ID")));
                        d.put("label", getString (row.get("GARD_Disease")));
                        related.add(d);
                    }
                    if (null != row.get("DO_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (row.get("DO_ID")));
                        d.put("label", getString (row.get("DO_Disease")));
                        related.add(d);
                    }
                    
                    return false;
                }, String.format(DISEASE_QUERY, value));
            if (!n.has("label")) {
                logger.warning("Can't get disease info for "+value+"!");
            }
            n.put("genes", mapper.createObjectNode());
            
            final ObjectNode phenos = mapper.createObjectNode();
            ef.cypher(row -> {
                    String hpid = getString (row.get("hpid"));
                    ObjectNode dp = mapper.createObjectNode();
                    set (dp, "label", row.get("label"));
                    set (dp, "evidence", row.get("evidence"));
                    phenos.put(hpid, dp);
                    
                    ObjectNode hpnode = hp.addPathsIfAbsent(hpid);
                    ObjectNode pd = mapper.createObjectNode();
                    set (pd, "label", n.get("label").asText());
                    set (pd, "evidence", row.get("evidence"));
                    //debug (value+"..."+hpid, hpnode);
                    ((ObjectNode)hpnode.get("diseases")).put(value, pd);
                    return true;
                }, "match (d)-[:PAYLOAD]->(n:S_OMIM)-[e:R_hasPhenotype]"
                +"->(:S_HP)<-[:PAYLOAD]-(z) "
                +"where z.hasOBONamespace='human_phenotype' and d.notation='"
                +value+"' return distinct z.label as label, "
                +"z.notation as hpid, e.Evidence as evidence order by hpid");
            n.put("phenotypes", phenos);
            
            node = n;
            if (postfn != null)
                postfn.accept(node);
            diseases.put(value, node);
        }
        return node;
    }
    
    public void build () throws Exception {
        ef.cypher(row -> {
                final String geneId = getString (row.get("Gene"));
                ObjectNode gene = createGeneIfAbsent
                    (geneId, n -> {
                        Long id = (Long)row.get("OGG1");
                        if (id == null) {
                            id = (Long)row.get("OGG2");
                        }
                        if (id == null) {
                            logger.warning("Gene "+geneId+" has no OGG node!");
                            ArrayNode terms = mapper.createArrayNode();
                            terms.add(geneId);
                            n.put("terms", terms);
                            set (n, "locus", row.get("OMIM_Locus"));
                            n.put("diseases", mapper.createObjectNode());
                        }
                        else {
                            Entity e = ef.getEntity(id);
                            instrumentGene (e, n);
                        }
                    });

                String diseaseId = getString (row.get("OMIM_ID"));
                ObjectNode disease = createDiseaseIfAbsent (diseaseId);
                
                logger.info(diseaseId+"..."+geneId);
                // gene -> disease
                ObjectNode gd = mapper.createObjectNode();
                gd.put("label", disease.get("label").asText());
                set (gd, "type", row.get("Type"));
                set (gd, "status", row.get("Status"));
                gd.put("phenotypes", disease.get("phenotypes").size());
                ((ObjectNode)gene.get("diseases")).put(diseaseId, gd);

                // disease -> gene
                ObjectNode dg = mapper.createObjectNode();
                set (dg, "type", row.get("Type"));
                set (dg, "status", row.get("Status"));
                ((ObjectNode)disease.get("genes")).put(geneId, dg);

                //debug (diseaseId+"..."+geneId, disease);

                return true;
            }, MONOGENIC_QUERY);
    }

    ObjectNode instrumentGene (Map<String, Object> row) {
        String gene = getString (row.get("Gene"));
        ObjectNode node = (ObjectNode)genes.get(gene);
        if (node == null) {
            final ObjectNode n = mapper.createObjectNode();
            //n.put("gene", gene);
            n.put("tdl", (String)row.get("TDL"));
            n.put("locus", (String)row.get("OGG_Locus"));
            n.put("functions", mapper.createObjectNode());
            n.put("diseases", mapper.createObjectNode());

            ObjectNode annotations = mapper.createObjectNode();
            ef.cypher(r -> {
                    String goid = getString (r.get("id"));
                    ObjectNode ag = mapper.createObjectNode();
                    set (ag, "label", r.get("label"));
                    set (ag, "type", r.get("ns"));
                    annotations.put(goid, ag);
                    
                    ObjectNode gn = go.addPathsIfAbsent(goid);
                    ObjectNode ga = mapper.createObjectNode();
                    set (ga, "tdl", n.get("tdl").asText());                    
                    set (ga, "locus", n.get("locus").asText());

                    ((ObjectNode)gn.get("genes")).put(gene, ga);
                    return true;
                }, "match (z)-[:PAYLOAD]->(n:S_OGG)-"
                +"[:R_rel{name:'has_go_association'}]->(:S_GO)"
                +"<-[:PAYLOAD]-(d) where z.label = '"+gene+"' "
                +"and not exists(d.deprecated) "
                +"return d.notation as id, "
                +"d.hasOBONamespace as ns, d.label as label");
            n.put("go_annotations", annotations);
            genes.put(gene, node = n);
            logger.info("+++ Instrumenting gene "+gene+"...");
        }

        ObjectNode diseases = (ObjectNode)node.get("diseases");
        String omim = (String)row.get("OMIM_ID");
        ObjectNode d = (ObjectNode)diseases.get(omim);
        ArrayNode types;
        if (d == null) {
            diseases.put(omim, d = mapper.createObjectNode());
            //d.put("curie", omim);
            d.put("label", (String)row.get("OMIM_Disease"));
            d.put("types", types = mapper.createArrayNode());
        }
        else {
            types = (ArrayNode)d.get("types");
        }

        ObjectNode functions = (ObjectNode)node.get("functions");
        Object t = row.get("Disease_Gene_Association_Type");
        if (t.getClass().isArray()) {
            int len = Array.getLength(t);
            for (int i = 0; i < len; ++i) {
                String v = (String)Array.get(t, i);
                boolean dup = false;
                for (int j = 0; j < types.size() && !dup; ++j) {
                    if (v.equals(types.get(j).asText()))
                        dup = true;
                }
                if (!dup) {
                    types.add(v);
                    JsonNode nv = functions.get(v);
                    if (nv != null)
                        functions.put(v, nv.asInt()+1);
                    else
                        functions.put(v, 1);
                }
            }
        }
        else {
            String v = (String)t;
            boolean dup = false;
            for (int j = 0; j < types.size() && !dup; ++j) {
                if (v.equals(types.get(j).asText()))
                    dup = true;
            }
                    
            if (!dup) {
                types.add(v);
                JsonNode nv = functions.get(v);
                if (nv != null)
                    functions.put(v, nv.asInt()+1);
                else
                    functions.put(v, 1);
            }
        }
        
        return node;
    }

    ObjectNode instrumentDisease (Map<String, Object> row) {
        String omim = getString (row.get("OMIM_ID"));
        ObjectNode node = (ObjectNode)diseases.get(omim);
        if (node == null) {
            final ObjectNode n = mapper.createObjectNode();
            ef.cypher(r -> {
                    n.put("label", getString (r.get("OMIM_Disease")));
                    n.put("type", getString (r.get("OMIM_Type")));
                    
                    ArrayNode related = mapper.createArrayNode();
                    n.put("related", related);
                    
                    ObjectNode d = mapper.createObjectNode();
                    d.put("curie", getString (r.get("MONDO_ID")));
                    d.put("label", getString (r.get("MONDO_Disease")));
                    related.add(d);
                    
                    if (null != r.get("Orphanet_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (r.get("Orphanet_ID")));
                        d.put("label", getString (r.get("Orphanet_Disease")));
                        related.add(d);
                    }
                    if (null != r.get("GARD_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (r.get("GARD_ID")));
                        d.put("label", getString (r.get("GARD_Disease")));
                        related.add(d);
                    }
                    if (null != r.get("DO_ID")) {
                        d = mapper.createObjectNode();
                        d.put("curie", getString (r.get("DO_ID")));
                        d.put("label", getString (r.get("DO_Disease")));
                        related.add(d);
                    }
                    return false;
                }, String.format(DISEASE_QUERY, omim));
            n.put("genes", mapper.createObjectNode());            
            
            final ObjectNode phenos = mapper.createObjectNode();
            ef.cypher(r -> {
                    String hpid = getString (r.get("hpid"));
                    ObjectNode dp = mapper.createObjectNode();
                    set (dp, "label", r.get("label"));
                    set (dp, "evidence", r.get("evidence"));
                    phenos.put(hpid, dp);
                    
                    ObjectNode hpnode = hp.addPathsIfAbsent(hpid);
                    ObjectNode pd = mapper.createObjectNode();
                    set (pd, "label", n.get("label").asText());
                    set (pd, "evidence", r.get("evidence"));
                    //debug (value+"..."+hpid, hpnode);
                    ((ObjectNode)hpnode.get("diseases")).put(omim, pd);
                    return true;
                }, "match (d)-[:PAYLOAD]->(n:S_OMIM)-[e:R_hasPhenotype]"
                +"->(:S_HP)<-[:PAYLOAD]-(z) "
                +"where z.hasOBONamespace='human_phenotype' and d.notation='"
                +omim+"' return distinct z.label as label, "
                +"z.notation as hpid, e.Evidence as evidence order by hpid");
            n.put("phenotypes", phenos);
            diseases.put(omim, node = n);
            logger.info("+++ Instrumenting disease "+omim+"...");
        }
        
        ObjectNode genes = (ObjectNode)node.get("genes");
        String gene = getString (row.get("Gene"));
        if (!genes.has(gene)) {
            ObjectNode g = mapper.createObjectNode();
            set (g, "tdl", row.get("TDL"));
            set (g, "locus", row.get("OGG_Locus"));
            ArrayNode types = mapper.createArrayNode();
            g.put("functions", types);
            Object t = row.get("Disease_Gene_Association_Type");
            if (t.getClass().isArray()) {
                int len = Array.getLength(t);
                for (int i = 0; i < len; ++i) {
                    String v = (String)Array.get(t, i);
                    types.add(v);
                }                
            }
            else {
                types.add((String)t);
            }
            genes.put(gene, g);
        }
        
        return node;
    }
    
    static boolean hasGoF (JsonNode node) {
        return node.has("functions") && node.get("functions")
            .has("Disease-causing germline mutation(s) (gain of function) in");
    }
    static boolean hasLoF (JsonNode node) {
        return node.has("functions") && node.get("functions")
            .has("Disease-causing germline mutation(s) (loss of function) in");
    }
    
    public void run () throws Exception {
        final Map<String, ObjectNode> genes = new TreeMap<>();
        ef.cypher(row -> {
                ObjectNode gene = instrumentGene (row);
                ObjectNode disease = instrumentDisease (row);
                return true;
            }, MONOGENIC_QUERY2);
    }

    public void writeJson (String file) throws Exception {
        try (PrintStream ps = new PrintStream (new FileOutputStream (file))) {
            ObjectNode root = mapper.createObjectNode();
            root.put("genes", genes);
            root.put("diseases", diseases);
            root.put("phenotypes", hp.vocab);
            root.put("go_annotations", go.vocab);
            root.put("hp_paths", hp.paths);
            root.put("go_paths", go.paths);
            mapper.writerWithDefaultPrettyPrinter().writeValue(ps, root);
        }
    }

    public void paths (PrintStream ps, String source, String hpid) {
        ps.println("++ paths for "+hpid+"...");
        ef.cypher(row -> {
                long id = (Long)row.get("id");
                for (Entity[] p : ef.parents(id, R_subClassOf)) {
                    for (int i = 0; i < p.length; ++i) {
                        for (int j = 0; j < i; ++j)
                            ps.print(" ");
                        ps.println(p[i].payload("notation")
                                   +" "+p[i].payload("label")+" ("
                                   +p[i].getId()+")");
                    }
                    ps.println();
                }
                return true;
            }, "match (d)-[:PAYLOAD]->(n:`"+source+"`) where d.notation='"
            +hpid.trim()+"' return id(n) as id");
    }
    
    public void paths (String source, String hpid) {
        paths (System.out, source, hpid);
    }

    Set<String> getGO (final Set<String> set, String query) {
        ef.cypher(row -> {
                long id = (Long)row.get("ID");
                for (Entity[] p : ef.parents(id, R_subClassOf))
                    for (Entity e : p)
                        set.add(getString (e.payload("notation")));
                set.add((String)row.get("goid"));
                return true;
            }, query);
        return set;
    }
    
    public void gosim () {
        Set<String> druggable = getGO
            (new TreeSet<>(), "match p=(d)-[:PAYLOAD]->(:`S_TCRD660`)-[:I_GENE]-(n:S_OGG)<-[:PAYLOAD]-(z) where d.tdl in ['Tclin','Tchem'] match q=(n)-[:R_rel{name:'has_go_association'}]->(m:S_GO)<-[:PAYLOAD]-(g) where g.hasOBONamespace='molecular_function' and not (m)<-[:R_subClassOf]-() and not exists(g.deprecated) return id(m) as ID, g.notation as goid");
        Set<String> monogenic = getGO
            (new TreeSet<>(), "match p=(d)-[:PAYLOAD]->(:`S_MONOGENIC DISEASE`)-[:I_GENE]-(n:S_OGG)<-[:PAYLOAD]-(z) match q=(n)-[:R_rel{name:'has_go_association'}]->(m:S_GO)<-[:PAYLOAD]-(g) where g.hasOBONamespace='molecular_function' and not (m)<-[:R_subClassOf]-() and not exists(g.deprecated) return id(m) as ID, g.notation as goid");
        Set<String> all = new HashSet<>();
        all.addAll(druggable);
        all.addAll(monogenic);
        druggable.retainAll(monogenic);
        logger.info("*** similarity = " + (float)druggable.size()/all.size());
    }

    public void hpcategories () {
        /* return distribution of 
         */
        final Map<Entity, Set<Long>> counts = new HashMap<>();
        ef.cypher(row -> {
                long id = (Long)row.get("id");
                long omim = (Long)row.get("omim");
                for (Entity[] p : ef.parents(id, R_subClassOf)) {
                    String hpid = getString (p[1].payload("notation"));
                    if ("HP:0000118".equals(hpid)) { // Phenotypic abnormality
                        counts.computeIfAbsent
                            (p[2], k -> new TreeSet<>()).add(omim);
                    }
                }
                return true;
            }, "match p=(d)-[:PAYLOAD]->(:`S_MONOGENIC DISEASE`)-[:I_CODE]-(m:S_OMIM)<-[:PAYLOAD]-(z) where z.MIMTYPE = '3' match (m)-[:R_hasPhenotype]->(n:S_HP)<-[:PAYLOAD]-(h) return id(m) as omim, id(n) as id");
        Set<Map.Entry<Entity, Set<Long>>> sorted = new TreeSet<>
            ((v1, v2) -> {
                int d = v2.getValue().size() - v1.getValue().size();
                if (d == 0) {
                    d =  v1.getKey().getId() > v2.getKey().getId() ? 1 : -1;
                }
                return d;
            });
        sorted.addAll(counts.entrySet());
        for (Map.Entry<Entity, Set<Long>> me : sorted) {
            Entity e = me.getKey();
            System.out.println(getString (e.payload("notation"))+","
                               +getString (e.payload("label"))+","
                               +me.getValue().size());
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+Monogenic.class.getName()+" DBDIR OUTPUT");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            Monogenic mono = new Monogenic (ef);
            //mono.paths("S_HP", "HP:0000068");
            //mono.paths("S_HP", "HP:0009466");
            //mono.paths("S_HP", " HP:0002982");
            //mono.run();
            //mono.build();
            //mono.writeJson(argv[1]);
            //mono.gosim();
            mono.hpcategories();
        }
    }
}
