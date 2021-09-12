package ncats.stitcher.disease;

import java.util.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.ontology.*;
import org.apache.jena.rdf.model.*;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.FileManager;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.ontology.OntModelSpec;
import org.apache.jena.vocabulary.OWL2;

public class GARDOntology {
    static final Logger logger = Logger.getLogger(GARDOntology.class.getName());

    static final String URI = "https://disease.ncats.io/gard.owl";
    static final String VERSION = "20210621";

    static class XRef {
        final String prefix;
        final String uri;
        final Map<String, Resource> resources = new TreeMap<>();
        XRef (String prefix, String uri) throws Exception {
            //logger.info("## loading reference "+uri+"...");
            OntModel model = ModelFactory.createOntologyModel
                (OntModelSpec.OWL_MEM);
            model.read(uri);
            
            //logger.info("--------- "+uri);
            //logger.info("[properties]");
            int props = 0;
            for (ExtendedIterator<OntProperty> it =
                     model.listAllOntProperties(); it.hasNext(); ++props) {
                OntProperty prop = it.next();
                Resource old = resources.put(prop.getLocalName(), prop);
                if (old != null) {
                    logger.warning("Property \""+prop.getLocalName()+"\" "
                                   +"overrides resource " + old);
                }
                //System.out.println(prop.getURI());
            }
            //logger.info("## "+uri+" has "+props+" properties!");
            //logger.info(" [classes]");
            int nc = loadClasses (model.listClasses());
            //logger.info("## "+uri+" has "+nc+" classes!");

            this.prefix = prefix;
            this.uri = uri;
        }

        int loadClasses (ExtendedIterator<OntClass> classes) {
            int nc = 0;
            for (ExtendedIterator<OntClass> it = classes; it.hasNext();) {
                OntClass clz = it.next();
                if (!clz.isAnon()) {
                    //System.out.println(clz.getLocalName()+": "+clz.getURI());
                    Resource old = resources.put(clz.getLocalName(), clz);
                    if (old != null) {
                        logger.warning("Class \""+clz
                                       +"\" overrides resource "+old);
                    }
                    ++nc;
                }
            }
            return nc;
        }

        public Resource resource (String name) {
            return resources.get(name);
        }
    }

    final Map<String, XRef> xrefs = new HashMap<>();
    final OntModel model;
    final Ontology ont;

    class GARDClass {
        final OntClass resource;
        GARDClass (OntClass resource) {
            this.resource = resource;
        }
        GARDClass addLabel (String label) {
            resource.addLabel(label, "en");
            return this;
        }
        GARDClass addProperty (Property p, String value) {
            resource.addProperty(p, value);
            return this;
        }
        GARDClass addProperty (Property p, String value, String lang) {
            resource.addProperty(p, value, lang);
            return this;
        }
        GARDClass addDescription (String desc) {
            return addProperty (p("dc", "description"), desc);
        }
        GARDClass addSubClass (Resource cls) {
            resource.addSubClass(cls);
            return this;
        }
        GARDClass addEquivalentClass (Resource cls) {
            resource.addEquivalentClass(cls);
            return this;
        }
        GARDClass addId (String id) {
            return addProperty (p("skos", "notation"), id);
        }
        GARDClass addSynonym (String syn) {
            return addSynonym (syn, "en");
        }
        GARDClass addSynonym (String syn, String lang) {
            return addProperty (p("oboInOwl", "hasSynonym"), syn, lang);
        }
        Resource createAxiom () {
            Resource ax = model.createResource(OWL2.Axiom);
            ax.addProperty(OWL2.annotatedSource, resource);
            return ax;
        }
    }

    public GARDOntology () throws Exception {
        model = ModelFactory.createOntologyModel();
        add("foaf", "http://xmlns.com/foaf/0.1/")
            .add("terms", "http://purl.org/dc/terms/")
            .add("dc", "http://purl.org/dc/elements/1.1/")
            .add("skos", "http://www.w3.org/2004/02/skos/core#")
            //.add("owl", "http://www.w3.org/2002/07/owl#")
            .add("oboInOwl", "http://www.geneontology.org/formats/oboInOwl#")
            .add("biolink", "https://data.bioontology.org/ontologies/BLM/download?apikey=8b5b7825-538d-40e0-9e9e-5ab9274a9aeb&download_format=rdf")
            ;
        model.setNsPrefix("biolink", "https://w3id.org/biolink/vocab/");

        /*
         * setup ontology metadata
         */
        ont = model.createOntology(URI);        
        ont.addVersionInfo(VERSION);
        //ont.setLabel("GARD disease ontology", "en");
        ont.addComment("Built on "+ new java.util.Date(), "en");
        ont.addProperty(p ("dc", "title"), "GARD disease ontology");
        ont.addProperty(p ("terms", "license"),
                        "http://creativecommons.org/licenses/by/4.0/");
        addSource ("http://purl.obolibrary.org/obo/mondo.owl")
            .addSource ("http://purl.obolibrary.org/obo/doid.owl/")
            .addSource ("http://purl.obolibrary.org/obo/hp.owl")
            .addSource ("http://www.orpha.net/ontology/orphanet.owl")
            .addSource ("http://purl.obolibrary.org/obo/ncit.owl")
            ;
    }

    public GARDOntology addSource (String uri) {
        ont.addProperty(p ("terms", "source"), createResource (uri));
        return this;
    }

    public OntResource createResource (String uri) {
        return model.createOntResource(uri);
    }

    public Individual createIndividual (String uri) {
        return model.createIndividual(createResource (uri));
    }

    public OntClass createClass (String uri) {
        return model.createClass(uri);
    }
        
    public Resource r (String ns, String name) {
        XRef ref = xrefs.get(ns);
        return ref != null ? ref.resource(name) : null;
    }
        
    public Property p (String ns, String name) {
        Resource r = r (ns, name);
        return r != null ? r.as(Property.class) : null;
    }

    public Literal l (String value) {
        return model.createLiteral(value, "en");
    }

    public OntClass c (String ns, String name) {
        Resource r = r (ns, name);
        return r != null ? r.as(OntClass.class) : null;
    }

    public GARDOntology add (XRef ref) {
        if (!xrefs.containsKey(ref.prefix)) {
            model.setNsPrefix(ref.prefix, ref.uri);
            xrefs.put(ref.prefix, ref);
        }
        else {
            logger.warning("** prefix \""+ref.prefix+"\" already loaded!");
        }
        return this;
    }

    public GARDOntology add (String prefix, String uri) throws Exception {
        return add (new XRef (prefix, uri));
    }

    public OntModel getModel () { return model; }
    public Ontology getOntology () { return ont; }
    public GARDClass createInstance (String uri, Resource type) {
        OntClass oc = createClass (uri);
        //oc.setRDFType(type);
        type.as(OntClass.class).addSubClass(oc);
        return new GARDClass (oc);
    }
    public GARDClass createGene (String id, String uri) {
        return createInstance(uri, c("biolink", "Gene")).addId(id);
    }
    public GARDClass createDisease (String id, String uri) {
        return createInstance(uri, c("biolink", "Disease")).addId(id);
    }
    public GARDClass createPhenotype (String id, String uri) {
        return createInstance(uri, c("biolink", "PhenotypicFeature"))
            .addId(id);
    }
    public GARDClass createDrug (String id, String uri) {
        return createInstance(uri, c("biolink", "Drug")).addId(id);
    }

    public void write (Writer writer) throws IOException {
        model.write(writer);
    }
    
    public static void main (String[] argv) throws Exception {
        //test ();
        GARDOntology g = new GARDOntology ();

        GARDClass c = g
            .createGene("HGNC:10023", "http://identifiers.org/hgnc/10023")
            .addLabel("RIT1")
            .addDescription("Ras like without CAAX 1")
            .addSynonym("RAS like w/o CAAX 1")
            ;
        c.createAxiom()
            .addProperty(OWL2.annotatedProperty, g.p("oboInOwl", "hasDbXref"))
            .addProperty(OWL2.annotatedTarget, g.l("ROC1"))
            ;
        //g.getModel().add(OWL2.Axiom, OWL2.annotatedSource, c.resource);
        //logger.info("Axiom => "+g.createClass("owl:Axiom"));
        
        g.getModel().write(System.out);
    }
    
    static void test () throws Exception {
        PrefixMapping prefix = PrefixMapping.Standard;
        System.out.println("Prefixes => "+prefix.getNsPrefixMap());

        OntModel dc = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
        dc.read("http://purl.org/dc/terms/");
        //System.out.println("DC model => " + dc);
        for (ExtendedIterator<OntProperty> it = dc.listAllOntProperties();
             it.hasNext();) {
            OntProperty prop = it.next();
            System.out.println("..."+prop+" ns="+prop.getNameSpace()
                               +" name="+prop.getLocalName());
        }
        Property source = dc.getProperty("http://purl.org/dc/terms/", "source");
        System.out.println("source => " + source.getURI());

        OntModel mondo = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM);
        mondo.read("mondo.owl");
        
        Ontology mondoOnt = mondo.getOntology
            ("http://purl.obolibrary.org/obo/mondo.owl");
        System.out.println("MONDO Ontology => "+mondoOnt);
        for (StmtIterator it = mondoOnt.listProperties(); it.hasNext(); ) {
            Statement stm = it.next();
            Property pred = stm.getPredicate();
            RDFNode obj = stm.getObject();
            Resource sub = stm.getSubject();
            //System.out.println("sub => "+sub);
            //System.out.println("pred => "+pred);
            //System.out.println("obj => "+obj);
            System.out.println(pred+": "+obj);
        }
        
        OntModel model = ModelFactory.createOntologyModel();
        //model.setNsPrefixes(prefix);
        model.setNsPrefix("terms", "http://purl.org/dc/terms/");
        //model.setNsPrefix("skos", "http://www.w3.org/2004/02/skos/core#");

        //Property p = source.inModel(model);
        Property p = model.getProperty("http://purl.org/dc/terms/", "source");

        Ontology ont = model.createOntology(URI);
        ont.addVersionInfo(VERSION);
        ont.setLabel("GARD disease ontology", "en");
        ont.addComment("Built on "+ new java.util.Date(), "en");
        ont.addProperty(source, model.createResource
                        ("http://purl.obolibrary.org/obo/mondo.owl"));
        model.write(System.out);

        System.out.println("------------");
    }
}
