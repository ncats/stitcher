package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
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
 * sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.DiseaseEntityFactory ordo.owl"
 */
public class DiseaseEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(DiseaseEntityFactory.class.getName());
    
    static final String FIELD_ID = "uri";
    static final Set<String> DEFERRED_FIELDS = new TreeSet<>();
    static {
        DEFERRED_FIELDS.add("subClassOf");
        DEFERRED_FIELDS.add("equivalentClass");
        DEFERRED_FIELDS.add("exactMatch");
        DEFERRED_FIELDS.add("closeMatch");
    }

    static class OntologyResource {
        final public Resource resource;
        final public String uri;
        final public String type;
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
                    Object v = obj.asLiteral().getValue();
                    if (!v.getClass().isAssignableFrom(Number.class))
                        v = v.toString();
                    if (!"".equals(v)) {
                        Object old = props.get(pname);
                        props.put(pname, old != null ? Util.merge(old, v) : v);
                    }
                }
            }
            this.uri = res.getURI();
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
        public boolean isOntology () {
            return "Ontology".equalsIgnoreCase(type);
        }

        public String toString () {
            StringBuilder sb = new StringBuilder ();
            sb.append("> ");
            if (isClass()) sb.append(resource.getLocalName()+" "+uri);
            else if (isAxiom()) sb.append(resource.getId());
            else sb.append(resource.toString());
            sb.append("\n");
            toString (sb, props);
            toString (sb, links);
            return sb.toString();
        }

        void toString (StringBuilder sb, Map<String, Object> values) {
            for (Map.Entry<String, Object> me : values.entrySet()) {
                sb.append("-- "+me.getKey()+":");
                Object value = me.getValue();
                if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); ++i) {
                        sb.append(" ["+Array.get(value, i)+"]");
                    }
                }
                else {
                    sb.append(" ["+value+"]");
                }
                sb.append("\n");
            }
        }
    }

    Map<Resource, OntologyResource> diseases = new HashMap<>();
    List<OntologyResource> others = new ArrayList<>();
    OntologyResource ontology;
    
    public DiseaseEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public DiseaseEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public DiseaseEntityFactory (File dir) throws IOException {
        super (dir);
    }

    static boolean isDeferred (String field) {
        return DEFERRED_FIELDS.contains(field);
    }
    
    @Override
    protected void init () {
        super.init();
        setIdField (FIELD_ID);
        setNameField ("label");
        add (N_Name, "label")
            .add(N_Name, "hasExactSynonym")
            .add(N_Name, "hasRelatedSynonym")
            .add(I_CODE, "hasDbXref")
            .add(I_CODE, "id")
            .add(I_CODE, "hasAlternativeId")
            .add(T_Keyword, "inSubset")
            .add(T_Keyword, "type")
            .add(T_Keyword, "hasOBONamespace")
            .addBlacklist (I_CODE, "MONDO:LEXICAL"
                           ,"MONDO:PATTERNS/DISEASE_SERIES_BY_GENE"
                           ,"MONDO:DESIGN_PATTERN"
                           )
            ;
    }

    protected void reset () {
        ontology = null;
        diseases.clear();
        others.clear();
    }
    
    static String getResourceValue (Resource r) {
        String v = r.getLocalName();
        return v != null ? v : r.getURI();
    }

    protected String transform (String value) {
        if (value.startsWith("UMLS_CUI")) {
            value = value.replaceAll("_CUI", "");
        }
        return value;
    }
    
    protected Entity _registerIfAbsent (OntologyResource or) {
        Map<String, Object> data = new TreeMap<>();
        data.put(FIELD_ID, or.uri);
        data.putAll(or.props);

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

        // don't use ICD code for stitching
        Object value = data.remove("hasDbXref");
        if (value != null) {
            List<String> xrefs = new ArrayList<>();
            List<String> icds = new ArrayList<>();
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i) {
                    String v = Array.get(value, i).toString();
                    if (v.startsWith("ICD"))
                        icds.add(v);
                    else
                        xrefs.add(transform (v));
                }
            }
            else {
                String v = value.toString();
                if (v.startsWith("ICD")) icds.add(v);
                else xrefs.add(transform (v));
            }
            
            if (!xrefs.isEmpty()) {
                String ns = ontology != null ? (String)ontology.props.get
                    ("default-namespace") : null;

                if ("human_phenotype".equals(ns)) {
                    // for hpo, hasDbXref in axiom corresponds to curator or 
                    // publication. don't stitch on these
                    List<String> others = new ArrayList<>();
                    List<String> useful = new ArrayList<>();
                    for (String u : xrefs) {
                        String x = u.toUpperCase();
                        // sigh.. non-informative xrefs; why are these xrefs?
                        if (x.startsWith("UMLS")
                            || (x.startsWith("HP:")
                                && !x.equals("HP:PROBINSON"))
                            || x.startsWith("SNOMEDCT")
                            || x.startsWith("MSH"))
                            useful.add(u);
                        else
                            others.add(u);
                    }
                    xrefs = useful;
                    if (!others.isEmpty())
                        data.put("_hasDbXref", others.toArray(new String[0]));
                }
                data.put("hasDbXref", xrefs.toArray(new String[0]));
            }
            
            if (!icds.isEmpty())
                data.put("ICD", icds.toArray(new String[0]));
        }

        return register (data);
    }

    boolean _stitch (Entity ent, String name, Resource res) {
        boolean stitched = false;
        String uri = res.getURI();
        if (uri != null) {
            Iterator<Entity> iter = find (FIELD_ID, uri);
            if (iter.hasNext()) {
                Entity e = iter.next();
                switch (name) {
                case "subClassOf":
                    stitched = ent._stitch(e, R_subClassOf, uri);
                    break;
                case "equivalentClass":
                    stitched = ent._stitch(e, R_equivalentClass, uri);
                    break;
                case "exactMatch":
                    stitched = ent._stitch(e, R_exactMatch, uri);
                    break;
                case "closeMatch":
                    stitched = ent._stitch(e, R_closeMatch, uri);
                    break;
                default:
                    logger.warning("Unknown stitch relationship: "+name);
                }
            }
        }
        return stitched;
    }

    protected void _resolve (OntologyResource or) {
        List<Entity> entities = new ArrayList<>();
        for (Iterator<Entity> iter = find (FIELD_ID, or.uri); iter.hasNext();) {
            Entity e = iter.next();
            entities.add(e);
        }

        if (!entities.isEmpty()) {
            for (Map.Entry<String, Object> me : or.links.entrySet()) {
                if (DEFERRED_FIELDS.contains(me.getKey())) {
                    Object value = me.getValue();
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        for (int i = 0; i < len; ++i) {
                            Resource res = (Resource) Array.get(value, i);
                            for (Entity e : entities)
                                _stitch (e, me.getKey(), res);
                        }
                    }
                    else {
                        Resource res = (Resource) value;
                        for (Entity e : entities)
                            _stitch (e, me.getKey(), res);
                    }
                }
            }
        }
        else {
            logger.warning("Unable to resolve "+or.uri);
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

    protected void resolve (OntologyResource or) {
        try (Transaction tx = gdb.beginTx()) {
            _resolve (or);
            tx.success();
        }
    }

    public DataSource register (String file) throws Exception {
        DataSource ds = super.register(new File (file));

        Model model = ModelFactory.createDefaultModel();
        model.read(file);

        reset ();        
        Set<OntologyResource> axioms = new HashSet<>();
        
        logger.info("Loading resources...");
        for (ResIterator iter = model.listSubjects(); iter.hasNext();) {
            Resource res = iter.next();
            
            OntologyResource or = new OntologyResource (res);
            if (or.isAxiom()) {
                res = (Resource) or.links.get("annotatedSource");
                OntologyResource disease = diseases.get(res);
                if (disease != null)
                    disease.axioms.add(or);
                else
                    axioms.add(or);
            }
            else if (or.isClass()) {
                if (or.uri != null)
                    diseases.put(res, or);
                else
                    logger.warning("Ignore class "+res);
            }
            else if (or.isOntology()) {
                ontology = or;
                for (Map.Entry<String, Object> me : or.props.entrySet())
                    if (!"".equals(me.getKey()))
                        ds.set(me.getKey(), me.getValue());
                logger.info(">>>>>>> Ontology <<<<<<<<\n"+or);
            }
            else {
                logger.warning("Resource type "
                               +or.type+" not recognized:\n"+or);
                others.add(or);
            }
        }

        List<OntologyResource> unresolved = new ArrayList<>();
        for (OntologyResource or : axioms) {
            Resource res = (Resource) or.links.get("annotatedSource");
            OntologyResource disease = diseases.get(res);
            if (disease != null) {
                disease.axioms.add(or);
            }
            else {
                //logger.warning("Unable to resolve axiom:\n"+or);
                unresolved.add(or);
            }
        }

        // register entities
        for (OntologyResource or : diseases.values()) {
            System.out.print(or);            
            System.out.println("..."+or.axioms.size()+" axiom(s)");
            for (OntologyResource ax : or.axioms) {
                System.out.print("   "+ax);
            }
            Entity ent = registerIfAbsent (or);
            System.out.println("+++++++ "+ent.getId()+" +++++++");
            /*
            if (ent.getId() > 1000l)
                break;
            */
        }

        // resolve entities
        for (OntologyResource or : diseases.values()) {
            resolve (or);
        }
        
        logger.info(diseases.size()+" disease classes!");
        if (!unresolved.isEmpty()) {
            logger.warning("!!!!! "+unresolved.size()
                           +" unresolved axioms !!!!!");
            for (OntologyResource or : unresolved)
                System.out.println(or);
        }

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
                    System.out.print(obj.asResource().getURI());
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
            logger.info("Usage: "+DiseaseEntityFactory.class.getName()
                        +" DBDIR [OWL|TTL]...");
            System.exit(1);
        }

        try (DiseaseEntityFactory def = new DiseaseEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) 
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
