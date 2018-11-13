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

/**
 * sbt -Djdk.xml.entityExpansionLimit=0 stitcher/"runMain ncats.stitcher.impl.DiseaseEntityFactory ordo.owl"
 */
public class DiseaseEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(DiseaseEntityFactory.class.getName());

    public DiseaseEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public DiseaseEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public DiseaseEntityFactory (File dir) throws IOException {
        super (dir);
    }

    protected Entity _register (Resource res) {
        Entity ent = null;
        String name = res.getLocalName();
        
        for (StmtIterator it = res.listProperties(); it.hasNext(); ) {
            Statement stm = it.next();
            Property prop = stm.getPredicate();
            RDFNode obj = stm.getObject();
            String pname = prop.getLocalName();
            System.out.print("-- "+pname+": ");
            if (obj.isResource()) {
                Resource r = obj.asResource();
                String rn = r.getLocalName();
                switch (pname) {
                case "type":
                    switch (rn) {
                    case "Restriction":
                        break;
                    case "Axiom":
                        break;
                    case "Class":
                        break;
                    }
                    break;
                case "equivalentClass":
                    break;
                case "annotatedSource":
                    break;
                case "annotatedProperty":
                    switch (rn) {
                    case "IAO_0000115":
                        break;
                    case "subClassOf":
                        break;
                    case "hasDbXref":
                        break;
                    case "hasRelatedSynonym":
                        break;
                    case "hasExactSynonym":
                        break;
                    case "seeAlso":
                        break;
                    }
                    break;
                case "annotatedTarget":
                    break;
                case "subClassOf":
                    break;
                case "exactMatch":
                    break;
                case "closeMatch":
                    break;
                case "onProperty":
                    break;
                case "someValuesFrom":
                    break;
                case "excluded_subClassOf":
                    break;
                }
            }
            else if (obj.isLiteral()) {
                switch (pname) {
                case "id":
                    break;
                case "label":
                    break;
                case "hasDbXref":
                    break;
                case "annotatedTarget":
                    break;
                case "hasExactSynonym":
                    break;
                case "hasRelatedSynonym":
                    break;
                case "source":
                    break;
                case "IAO_0000115": // should consult AnnotatedProperty
                    break;
                    
                }
                    
                Object val = obj.asLiteral().getValue();
                if (val != null) {
                    Object v = ent.get(pname);
                }
            }
            else {
                logger.warning("Unknown property: "+obj);
            }
        }
        return ent;
    }

    public Entity register (Resource res) {
        try (Transaction tx = gdb.beginTx()) {
            Entity ent = _register (res);
            tx.success();
            return ent;
        }
    }
    
    public DataSource register (String file) throws Exception {
        DataSource ds = super.register(new File (file));

        Model model = ModelFactory.createDefaultModel();
        model.read(file);
        for (ResIterator iter = model.listSubjects(); iter.hasNext();) {
            Resource res = iter.next();
            Entity ent = register (res);
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
        if (argv.length == 0) {
            logger.info("Usage: "+DiseaseEntityFactory.class.getName()
                        +" FILE");
            System.exit(1);
        }

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
    }
}
