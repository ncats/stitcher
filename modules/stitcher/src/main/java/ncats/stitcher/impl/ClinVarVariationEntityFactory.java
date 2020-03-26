package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
import java.util.regex.Matcher;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
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
 * sbt stitcher/"runMain ncats.stitcher.impl.ClinVarVariationEntityFactory DB ClinVarVariationRelease_00-latest.xml.gz"
 */
public class ClinVarVariationEntityFactory extends EntityRegistry {
    static public final String SOURCE_NAME = "CLINVAR";
    static public final String SOURCE_LABEL = "S_"+SOURCE_NAME;
    
    static final Logger logger =
        Logger.getLogger(ClinVarVariationEntityFactory.class.getName());

    static final boolean LITERATURE_ONLY = true;

    class ClinVarVariationParser implements BiConsumer<XmlStream, byte[]> {
        final DocumentBuilder builder;
        
        ClinVarVariationParser () throws Exception {
            builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
        }

        public void accept (XmlStream xs, byte[] xml) {
            //logger.info("### processing record "+xs.getCount()+"...");
            try {
                Document doc = builder.parse(new ByteArrayInputStream (xml));
                register (xs, doc, xml);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE,
                           "Can't register ClinVarVariation record:\n"
                           +new String(xml), ex);
            }
        }
    }

    DateFormat df = new SimpleDateFormat ("yyyy-MM-dd");

    public ClinVarVariationEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public ClinVarVariationEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public ClinVarVariationEntityFactory (File dir) throws IOException {
        super (dir);
    }
    
    @Override
    protected void init () {
        super.init();
        setIdField ("accession");
        setNameField ("name");
        add (I_GENE, "genes")
            .add(T_Keyword, "type")
            .add(T_Keyword, "species")
            .add(T_Keyword, "interpretations")
            ;
    }

    Entity register (XmlStream xs, Document doc, byte[] xml) throws Exception {
        XPath xpath = XPathFactory.newInstance().newXPath();        
        Element vcv = doc.getDocumentElement();

        if (LITERATURE_ONLY) {
            NodeList nodes = (NodeList)xpath.evaluate
                ("//InterpretedRecord/ClinicalAssertionList/ClinicalAssertion/ClinVarAccession[@SubmitterName=\"OMIM\"]", vcv, XPathConstants.NODESET);
            if (0 == nodes.getLength())
                return null;
        }

        Map<String, Object> data = new LinkedHashMap<>();
        String type = vcv.getAttribute("VariationType");
        data.put("accession", vcv.getAttribute("Accession"));
        data.put("name", vcv.getAttribute("VariationName"));
        data.put("type", type);
        data.put("version", Integer.parseInt(vcv.getAttribute("Version")));
        data.put("status", xpath.evaluate("//RecordStatus", vcv));
        data.put("species", xpath.evaluate("//Species", vcv));
                 
        String value = vcv.getAttribute("DateCreated");
        if (value != null) {
            Date date = df.parse(value);
            data.put("created", date.getTime());
        }
        
        value = vcv.getAttribute("DateLastUpdated");
        if (value != null) {
            Date date = df.parse(value);
            data.put("updated", date.getTime());
        }
        
        NodeList values = (NodeList)xpath.evaluate
            ("//InterpretedRecord/"
             +(type.equalsIgnoreCase("haplotype") ? "Haplotype/":"")
             +"SimpleAllele/GeneList/Gene", vcv, XPathConstants.NODESET);
        Set<String> genes = new TreeSet<>();
        Set<String> generefs = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element gene = (Element)values.item(i);
            String loc = xpath.evaluate("//Location/CytogeneticLocation", gene);
            String sym = gene.getAttribute("Symbol");
            genes.add(sym);
            generefs.add(sym);
            String id = gene.getAttribute("GeneID");
            if (id.length() > 0)
                generefs.add("GENE:"+id); // ncbi
            id = gene.getAttribute("HGNC_ID");
            if (id.length() > 0)
                generefs.add(id);
            id = xpath.evaluate("//OMIM", gene).trim();
            if (id.length() > 0)
                generefs.add("OMIM:"+id);
        }
        data.put("genes", generefs.toArray(new String[0]));
        data.put("gene_count", genes.size());

        List<String> rcv = new ArrayList<>();
        values = (NodeList)xpath.evaluate("//RCVList/RCVAccession/@Accession",
                                          vcv, XPathConstants.NODESET);
        for (int i = 0; i < values.getLength(); ++i) {
            rcv.add(((Attr)values.item(i)).getValue());
        }
        data.put("RCV", rcv.toArray(new String[0]));

        values = (NodeList)xpath.evaluate("//Interpretations/Interpretation",
                                          vcv, XPathConstants.NODESET);
        List<Map> interps = new ArrayList<>();
        Set<String> conditions = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element elm = (Element)values.item(i);
            NodeList cites = (NodeList)xpath.evaluate
                ("//Citation/ID[@Source=\"PubMed\"]",
                 elm, XPathConstants.NODESET);
            List<String> pmids = new ArrayList<>();
            for (int j = 0; j < cites.getLength(); ++j)
                pmids.add("PMID:"+((Element)cites.item(j)).getTextContent());
            NodeList refs = (NodeList)xpath.evaluate
                ("//ConditionList/TraitSet/Trait/XRef", elm,
                 XPathConstants.NODESET);
            Set<String> xrefs = new TreeSet<>();
            for (int j = 0; j < refs.getLength(); ++j) {
                Element r = (Element)refs.item(j);
                String db = r.getAttribute("DB");
                switch (db) {
                case "Orphanet":
                    xrefs.add("ORPHA:"+r.getAttribute("ID"));
                    break;
                case "MedGen":
                    xrefs.add("UMLS:"+r.getAttribute("ID"));
                    break;
                case "OMIM":
                    xrefs.add("OMIM:"+r.getAttribute("ID"));
                    break;
                case "HP":
                    xrefs.add(r.getAttribute("ID"));
                    break;
                case "Office of Rare Diseases":
                    xrefs.add(String.format
                              ("GARD:%1$07d",
                               Integer.parseInt(r.getAttribute("ID"))));
                    break;
                }
            }
            conditions.addAll(xrefs);
            
            Map interp = new LinkedHashMap ();
            interp.put("interp", xpath.evaluate("//Description", elm));
            interp.put("pmids", pmids);
            interp.put("xrefs", xrefs);
            interps.add(interp);
        }
        
        Set<String> interpretations =
            (Set)interps.stream().map(interp -> interp.get("interp"))
            .collect(Collectors.toSet());
        data.put("interpretations", interpretations.toArray(new String[0]));
        data.put("conditions", conditions.toArray(new String[0]));

        Entity ent = register (data);
        if (ent != null) {
            logger.info("++++++ "+String.format("%1$6d ", xs.getCount())
                        +data.get("accession")+": genes="+genes
                        +" interps="+interps.size()
                        +" conditions="+conditions.size()
                        +" "+data.get("name"));
            for (Map interp : interps) {
                Map attrs = new LinkedHashMap ();
                List<String> pmids = (List)interp.get("pmids");
                if (pmids != null && !pmids.isEmpty()) {
                    attrs.put("citations", pmids.toArray(new String[0]));
                }
                attrs.put(Props.NAME, "has_allelic_variant");
                attrs.put("interpretation", interp.get("interp"));
                Set<String> refs = (Set)interp.get("xrefs");                
                for (String r : refs) {
                    for (Iterator<Entity> iter = find (I_CODE, r);
                         iter.hasNext(); ) {
                        Entity e = iter.next();
                        if (!e.is(SOURCE_LABEL)) {
                            // don't stitch to other clinvar entity
                            logger.info("..."+ent.getId()+" -> "+r);
                            e.stitch(ent, R_rel, r, attrs);
                        }
                    }
                }
            }
        }

        return ent;
    }

    @Override
    public DataSource register (File file) {
        DataSource ds = getDataSourceFactory().register(SOURCE_NAME);
        setDataSource (ds);
        ds.set(Props.URI, file.toURI().toString());

        logger.info("############## registering entities for "+file);
        try (XmlStream xs = new XmlStream
             (new GZIPInputStream (new FileInputStream (file)),
              "VariationArchive", new ClinVarVariationParser ())) {
            int count = xs.start();
            ds.set(INSTANCES, count);
            updateMeta (ds);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse file: "+file, ex);
        }
        return ds;
    }

    public static void main(String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+ClinVarVariationEntityFactory.class.getName()
                        +" DBDIR ClinVarVariationRelease_00-latest.xml.gz");
            System.exit(1);
        }

        try (ClinVarVariationEntityFactory cvv =
             new ClinVarVariationEntityFactory (argv[0])) {
            cvv.register(new File (argv[1]));
        }
    }

    public static class Summary {
        static int count = 0, total = 0;
        final static Map<String, Integer> counts = new TreeMap<>();
        final static Map<Integer, Integer> genes = new TreeMap<>();

        static void stats () {
            Set<String> submitters =
                new TreeSet<>((a,b) -> counts.get(b) - counts.get(a));
            submitters.addAll(counts.keySet());
            System.out.println("############ "+total
                               +" variation(s) spanning the "
                               +"following submitters...");
            for (String s : submitters) {
                System.out.println("..."
                                   +String.format("%1$6d %2$s",
                                                  counts.get(s), s));
            }
            System.out.println("*** Gene distribution: "+genes);
            System.out.println();
        }
        
        public static void main (String[] argv) throws Exception {
            if (argv.length == 0) {
                logger.info("Usage: "+ClinVarVariationEntityFactory.Summary.class.getName()+" ClinVarVariationRelease_00-latest.xml.gz");
                System.exit(1);
            }

            final DocumentBuilder builder =
                DocumentBuilderFactory.newInstance().newDocumentBuilder();
            final XPath xpath = XPathFactory.newInstance().newXPath();

            try (XmlStream xs = new XmlStream
                 (new GZIPInputStream (new FileInputStream (argv[0])),
                  "VariationArchive", (_xs, xml) -> {
                     ++total;
                     try {
                         Document doc = builder.parse
                         (new ByteArrayInputStream (xml));
                         Element vcv = doc.getDocumentElement();
                         /*
                         NodeList nodes = (NodeList)xpath.evaluate
                         ("//InterpretedRecord/ClinicalAssertionList/ClinicalAssertion/ClinVarAccession[@SubmitterName=\"OMIM\"]", vcv, XPathConstants.NODESET);
                         if (nodes.getLength() > 0) {
                             ++count;
                             logger.info("++++ "+count+"/"+total+": "
                                         +vcv.getAttribute("VariationName"));
                         }
                         */
                         String sub = xpath.evaluate("//InterpretedRecord/ClinicalAssertionList/ClinicalAssertion/ClinVarAccession/@SubmitterName", vcv);
                         Integer cnt = counts.get(sub);
                         counts.put(sub, cnt == null ? 1 : cnt+1);

                         NodeList values = (NodeList)xpath.evaluate
                             ("//InterpretedRecord/SimpleAllele/GeneList/Gene",
                              vcv, XPathConstants.NODESET);
                         Set<String> ug = new TreeSet<>();
                         for (int i = 0; i < values.getLength(); ++i) {
                             Element gene = (Element)values.item(i);
                             ug.add(gene.getAttribute("Symbol"));
                         }

                         cnt = genes.get(ug.size());
                         genes.put(ug.size(), cnt == null ? 1 : cnt+1);

                         if (total % 10000 == 0) {
                             stats ();
                         }                         
                     }
                     catch (Exception ex) {
                         logger.log(Level.SEVERE, "Can't parse xml\n"
                                    +new String(xml), ex);
                     }
                 })) {
                xs.start();
                stats ();
            }
        }
    }

    public static class Extract {
        static int matches = 0;
        public static void main (String[] argv) throws Exception {
            if (argv.length < 2) {
                logger.info("Usage: "+ClinVarVariationEntityFactory.Extract.class.getName()+" ClinVarVariationRelease_00-latest.xml.gz VCVXX...");
                System.exit(1);
            }

            final DocumentBuilder builder =
                DocumentBuilderFactory.newInstance().newDocumentBuilder();
            
            final Set<String> accessions = new HashSet<>();
            for (int i = 1; i < argv.length; ++i)
                accessions.add(argv[i]);
            
            try (XmlStream xs = new XmlStream
                 (new GZIPInputStream (new FileInputStream (argv[0])),
                  "VariationArchive", (_xs, xml) -> {
                     try {
                         Document doc = builder
                            .parse(new ByteArrayInputStream (xml));
                         Element vcv = doc.getDocumentElement();
                         if (accessions.remove(vcv.getAttribute("Accession"))) {
                             System.out.println(new String (xml));
                             if (accessions.isEmpty()) {
                                 _xs.setDone(true);
                             }
                         }
                     }
                     catch (Exception ex) {
                         logger.log(Level.SEVERE,
                                    "Can't parse file "+argv[0], ex);
                     }
                 })) {
                xs.start();
                if (!accessions.isEmpty()) {
                    logger.warning("** Couldn't find the following "
                                   +"accessions: "+accessions);
                }
            }
        }
    }
}
