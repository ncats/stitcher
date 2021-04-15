package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import javax.xml.xpath.*;
import javax.xml.parsers.*;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import org.w3c.dom.Document;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class NewbornScreeningEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(NewbornScreeningEntityFactory.class.getName());

    XPath xpath = XPathFactory.newInstance().newXPath();    
    Map<String, Entity> entities = new HashMap<>();
    Element root;
    
    public NewbornScreeningEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public NewbornScreeningEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public NewbornScreeningEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setNameField ("name");
        setIdField ("key");
        add (N_Name, "name")
            .add(N_Name, "synonyms")
            .add(I_CODE, "xrefs")
            ;
    }

    void parseConditions () throws Exception {
        NodeList conditions = (NodeList)xpath.evaluate
            ("//newborn-screening-codes/conditions/condition", root,
             XPathConstants.NODESET);
        for (int i = 0; i < conditions.getLength(); ++i) {
            Element cond = (Element)conditions.item(i);
            String key = xpath.evaluate("./xml-file-key", cond);
            String name = xpath.evaluate("./name", cond);
            logger.info("## parsing "+key+"...");
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("key", key);
            data.put("name", name);
            data.put("description", xpath.evaluate("./description", cond));
            data.put("abbreviation",
                     xpath.evaluate("./abbreviation", cond));
            data.put("created-date",
                     xpath.evaluate("./created-date", cond));
            data.put("modified-date",
                     xpath.evaluate("./modified-date", cond));
            data.put("web-page", xpath.evaluate("./web-page", cond));
            Set<String> syns = new TreeSet<>();
            Set<String> xrefs = new TreeSet<>();
            NodeList nodes = (NodeList)xpath.evaluate
                ("./xref", cond, XPathConstants.NODESET);
            next: for (int k = 0; k < nodes.getLength(); ++k) {
                Element n = (Element)nodes.item(k);
                String db = xpath.evaluate("./db", n);
                String val = xpath.evaluate("./dbkey", n);
                if (!val.equals("") && !val.equals("N/A")) {
                    switch (db) {
                    case "sachdnc-cat":
                        data.put(db, val);
                        break next;
                    case "ghr":
                        break;
                    case "enzyme-code":
                        xrefs.add("EC:"+val);
                        break;
                    case "umls-cui":
                        xrefs.add("UMLS:"+val);
                        break;
                    case "uniprot":
                        xrefs.add("UNIPROTKB:"+val);
                        break;
                    case "snomed-ct":
                        xrefs.add("SNOMEDCT_US:"+val);
                        xrefs.add("SNOMEDCT:"+val);
                        break;
                    default:
                        xrefs.add(db.toUpperCase()+":"+val);
                    }
                    syns.add(xpath.evaluate("./memo", n));
                }
            }
            data.put("synonyms", syns.toArray(new String[0]));
            data.put("xrefs", xrefs.toArray(new String[0]));
            Entity e = register (data);
            if (e != null) {
                logger.info("++++++ "+e.getId()+": "+name+" ("+key+")");
                e.addLabel("CONDITION");
                entities.put(key, e);
            }
        }
        logger.info("** "+conditions.getLength()+" conditions!");
    }

    void parseMeasurements () throws Exception {
        NodeList measurements = (NodeList)xpath.evaluate
            ("//newborn-screening-codes/measurements/measurement", root,
             XPathConstants.NODESET);
        for (int i = 0; i < measurements.getLength(); ++i) {
            Element measure = (Element)measurements.item(i);
            String key = xpath.evaluate("./xml-file-key", measure);
            String name = xpath.evaluate("./name", measure);
            Map<String, Object> data = new LinkedHashMap<>();
            data.put("key", key);
            data.put("created-date", xpath.evaluate("./created-date", measure));
            data.put("modified-date",
                     xpath.evaluate("./modified-date", measure));
            data.put("web-page", xpath.evaluate("./web-page", measure));
            data.put("name", name);
            data.put("short-name", xpath.evaluate("./short-name", measure));
            data.put("units", xpath.evaluate("./units", measure));
            Set<String> syns = new TreeSet<>();
            Set<String> xrefs = new TreeSet<>();
            NodeList nodes = (NodeList)xpath.evaluate
                ("./xref", measure, XPathConstants.NODESET);
            for (int k = 0; k < nodes.getLength(); ++k) {
                Element n = (Element)nodes.item(k);
                syns.add(xpath.evaluate("./memo", n));
                xrefs.add(xpath.evaluate("./db", n).toUpperCase()+":"
                          +xpath.evaluate("./dbkey", n));
            }
            data.put("synonyms", syns.toArray(new String[0]));
            data.put("xrefs", xrefs.toArray(new String[0]));
            Entity e = register (data);
            if (e != null) {
                logger.info("++++++ "+e.getId()+": "+name+" ("+key+")");
                e.addLabel("MEASUREMENT");
                entities.put(key, e);                
            }
        }
        logger.info("** "+measurements.getLength()+" measurements!");
    }

    void parseMarkers () throws Exception {
        NodeList markers = (NodeList)xpath.evaluate
            ("//newborn-screening-codes/markers/marker", root,
             XPathConstants.NODESET);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(NAME, "marker");
        for (int i = 0; i < markers.getLength(); ++i) {
            Element marker = (Element)markers.item(i);
            String cond = xpath.evaluate("./condition-xml-file-key", marker);
            String measure = xpath.evaluate
                ("./measurement-xml-file-key", marker);
            Entity c = entities.get(cond);
            Entity m = entities.get(measure);
            if (c == null) {
                logger.log(Level.SEVERE, "Condition "+cond+" not found!");
            }
            if (m == null) {
                logger.log(Level.SEVERE, "Measurement "+measure+" not found!");
            }

            if (c != null && m != null) {
                c.stitch(m, R_rel, cond+"/"+measure, attrs);
            }
        }
        logger.info("** "+markers.getLength()+" markers!");        
    }
    
    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = getDataSourceFactory().register("NEWBORN", file);
        setDataSource (ds);

        try {
            DocumentBuilder builder = DocumentBuilderFactory
                .newInstance().newDocumentBuilder();
            root = builder.parse(ds.openStream()).getDocumentElement();
            String version = xpath.evaluate
                ("//newborn-screening-codes/last-updated-date", root);
            logger.info("** version: " + version);
            parseConditions ();
            parseMeasurements ();
            parseMarkers ();
            ds.set("version", version);
            ds.set(INSTANCES, entities.size());
            updateMeta (ds);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse XML file: "+file, ex);
        }
        return ds;
    }


    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+NewbornScreeningEntityFactory.class.getName()
                        +" DBDIR FILE");
            System.exit(1);
        }
        logger.info("Registering "+argv[0]+"...");
        try (NewbornScreeningEntityFactory ns =
             new NewbornScreeningEntityFactory (argv[0])) {
            DataSource ds = ns.register(new File (argv[1]));
        }
    }
}
