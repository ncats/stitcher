package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.lang.reflect.Array;
import java.util.zip.GZIPInputStream;

import javax.xml.parsers.*;
import javax.xml.xpath.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.w3c.dom.Attr;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/**
 * latest GTR is available at https://ftp.ncbi.nlm.nih.gov/pub/GTR/data/
 * sbt stitcher/"runMain ncats.stitcher.impl.GTREntityFactory DB gtr_ftp.xml.gz"
 */
public class GTREntityFactory extends EntityRegistry {
    static public final String SOURCE_NAME = "GTR";
    static public final String SOURCE_LABEL = "S_"+SOURCE_NAME;

    static final Logger logger =
        Logger.getLogger(GTREntityFactory.class.getName());

    class GTRParser implements BiConsumer<XmlStream, byte[]> {
        final DocumentBuilder builder;

        public int count;
        GTRParser () throws Exception {
            builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
        }

        public void accept (XmlStream xs, byte[] xml) {
            //logger.info("### processing record "+xs.getCount()+"...");
            try {
                Document doc = builder.parse(new ByteArrayInputStream (xml));
                Entity ent = register (xs, doc, xml);
                if (ent != null) {
                    //if (++count > 1000)
                    //  xs.setDone(true);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE,
                           "Can't register GTR record:\n"
                           +new String(xml), ex);
            }
        }
    }

    DateFormat df = new SimpleDateFormat ("yyyy-MM-dd");
    
    public GTREntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public GTREntityFactory (String dir) throws IOException {
        super (dir);
    }

    public GTREntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("id");
        setNameField ("name");
        add (N_Name, "diseases")
            .add(I_CODE, "xrefs")
            .add(T_Keyword, "mechanism")
            .add(T_Keyword, "type")
            .add(T_Keyword, "purposes")
            ;
    }

    Entity register (XmlStream xs, Document doc, byte[] xml) throws Exception {
        XPath xpath = XPathFactory.newInstance().newXPath();
        Element gtr = doc.getDocumentElement();

        Map<String, Object> data = new LinkedHashMap<>();
        String accession = gtr.getAttribute("GTRAccession");
        data.put("id", Long.parseLong(gtr.getAttribute("id")));
        data.put("accession", accession);
        data.put("LastUpdate", gtr.getAttribute("LastUpdate"));

        NodeList values = (NodeList)xpath.evaluate
            ("./TestName", gtr, XPathConstants.NODESET);
        if (values.getLength() > 0) {
            data.put("name", ((Element)values.item(0)).getTextContent());
        }
        
        Set<String> diseases = new TreeSet<>();
        Set<String> xrefs = new TreeSet<>();
        values = (NodeList)xpath.evaluate
            ("./ClinVarSet/ClinVarAssertion/TraitSet/Trait[@Type=\"Disease\"]",
             gtr, XPathConstants.NODESET);
        for (int i = 0; i < values.getLength(); ++i) {
            Element trait = (Element)values.item(i);
            NodeList names = (NodeList)xpath.evaluate
                ("./Name", trait, XPathConstants.NODESET);
            for (int j = 0; j < names.getLength(); ++j)
                diseases.add(((Element)names.item(j)).getTextContent());
            NodeList nl = (NodeList)xpath.evaluate
                ("./XRef", trait, XPathConstants.NODESET);
            for (int j = 0; j < nl.getLength(); ++j) {
                Element xref = (Element)nl.item(j);
                String db = xref.getAttribute("DB");
                String id = xref.getAttribute("ID");
                String xf = null;
                switch (db) {
                case "OMIM": xf = "OMIM:"+id; break;
                case "Orphanet": xf = "ORPHA:"+id; break;
                case "MedGen":
                    if (id.startsWith("CN"))
                        xf = "MEDGEN:"+id;
                    else
                        xf = "UMLS:"+id;
                    break;
                case "Human Phenotype Ontology":
                case "MONDO":
                    xf = id;
                    break;
                case "Office of Rare Diseases":
                    xf = String.format("GARD:%1%07d", Integer.parseInt(id));
                    break;
                }
                if (xf != null)
                    xrefs.add(xf);
            }
        }
        if (!diseases.isEmpty())
            data.put("diseases", diseases.toArray(new String[0]));
        if (!xrefs.isEmpty())
            data.put("xrefs", xrefs.toArray(new String[0]));

        values = (NodeList)xpath.evaluate
            ("./Indications", gtr, XPathConstants.NODESET);
        Set<String> purpose = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element ind = (Element)values.item(i);
            data.put("type", xpath.evaluate("./TestType", ind));
            String mech = xpath.evaluate("./Mechanism/Value", ind);
            if (!"".equals(mech))
                data.put("mechanism", mech);
            NodeList nl = (NodeList)xpath.evaluate
                ("./Purpose", ind, XPathConstants.NODESET);
            for (int j = 0; j < nl.getLength(); ++j)
                purpose.add(((Element)nl.item(j)).getTextContent());
        }
        if (!purpose.isEmpty())
            data.put("purposes", purpose.toArray(new String[0]));

        Entity ent = register (data);
        if (ent != null) {
            logger.info("++++++ "+String.format("%1$6d ", xs.getCount())
                        +data.get("accession")+": "+data.get("name")
                        +" xrefs="+xrefs.size());
        }
        return ent;
    }
    
    @Override
    public DataSource register (File file) {
        DataSource ds = getDataSourceFactory().register(SOURCE_NAME);
        setDataSource (ds);
        ds.set(Props.URI, file.toURI().toString());

        logger.info("############## registering entities for "+file);
        try {
            GTRParser gtr = new GTRParser ();
            try (XmlStream xs = new XmlStream
                 (new GZIPInputStream (new FileInputStream (file)),
                  "GTRLabTest", gtr)) {
                int total = xs.start();
                ds.set(INSTANCES, gtr.count);
                updateMeta (ds);
                logger.info("############### "+gtr.count+"/"
                            +total+" entities registered!");
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return ds;
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+GTREntityFactory.class.getName()
                        +" DBDIR gtr_ftp.xml.gz");
            System.exit(1);
        }

        try (GTREntityFactory gef =new GTREntityFactory (argv[0])) {
            gef.register(new File (argv[1]));
        }
    }
}
