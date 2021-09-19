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

    static final boolean LITERATURE_ONLY = false;

    class ClinVarVariationParser implements BiConsumer<XmlStream, byte[]> {
        final DocumentBuilder builder;

        public int count;
        ClinVarVariationParser () throws Exception {
            builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
        }

        public void accept (XmlStream xs, byte[] xml) {
            //logger.info("### processing record "+xs.getCount()+"...");
            try {
                Document doc = builder.parse(new ByteArrayInputStream (xml));
                Entity ent = register (xs, doc, xml);
                if (ent != null) {
                    //if (++count > 4000)
                    //    xs.setDone(true);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE,
                           "Can't register ClinVarVariation record:\n"
                           +new String(xml), ex);
            }
        }
    }

    DateFormat df = new SimpleDateFormat ("yyyy-MM-dd");
    PrintStream out;

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
    public void shutdown () {
        super.shutdown();
        if (out != null) {
            out.close();
        }
    }
    
    @Override
    protected void init () {
        super.init();
        setIdField ("id");
        setNameField ("name");
        add (I_GENE, "genes")
            .add(T_Keyword, "type")
            .add(T_Keyword, "species")
            .add(T_Keyword, "interpretations")
            .add(T_Keyword, "consequences")
            ;
        try {
            out = new PrintStream (new FileOutputStream ("clinvar_summary.txt"));
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    Entity register (XmlStream xs, Document doc, byte[] xml) throws Exception {
        XPath xpath = XPathFactory.newInstance().newXPath();        
        Element vcv = doc.getDocumentElement();

        String type = vcv.getAttribute("VariationType");
        String record = "./InterpretedRecord/"
            +(type.equalsIgnoreCase("haplotype") ? "Haplotype/":"");
        
        NodeList values = (NodeList)xpath.evaluate
            (record+"SimpleAllele/FunctionalConsequence",
             vcv, XPathConstants.NODESET);
        Set<String> funcs = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element func = (Element)values.item(i);
            funcs.add(func.getAttribute("Value"));
        }
        
        NodeList nodes = (NodeList)xpath.evaluate
            ("//InterpretedRecord/ClinicalAssertionList/ClinicalAssertion/ClinVarAccession[@SubmitterName=\"OMIM\"]", vcv, XPathConstants.NODESET);
        boolean omim = nodes.getLength() > 0;
        if (LITERATURE_ONLY && !omim)
            return null;

        Map<String, Object> data = new LinkedHashMap<>();
        String accession = vcv.getAttribute("Accession");
        data.put("id", Long.parseLong(vcv.getAttribute("VariationID")));
        data.put("accession", accession);
        data.put("name", vcv.getAttribute("VariationName"));
        data.put("type", type);
        data.put("version", Integer.parseInt(vcv.getAttribute("Version")));
        data.put("status", xpath.evaluate("./RecordStatus", vcv));
        data.put("species", xpath.evaluate("./Species", vcv));
                 
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

        values = (NodeList)xpath.evaluate
            (record+"SimpleAllele/GeneList/Gene", vcv, XPathConstants.NODESET);
        Set<String> genes = new TreeSet<>();
        Set<String> generefs = new TreeSet<>();
        Set<Integer> alleles = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element gene = (Element)values.item(i);
            String loc = xpath.evaluate("./Location/CytogeneticLocation", gene);
            String sym = gene.getAttribute("Symbol");
            genes.add(sym);
            generefs.add(sym);
            String id = gene.getAttribute("GeneID");
            if (id.length() > 0)
                generefs.add("GENE:"+id); // ncbi
            id = gene.getAttribute("HGNC_ID");
            if (id.length() > 0)
                generefs.add(id);
            id = xpath.evaluate("./OMIM", gene).trim();
            if (id.length() > 0)
                generefs.add("OMIM:"+id);

            String allele = xpath.evaluate("../../@AlleleID", gene);
            if (!"".equals(allele)) {
                alleles.add(Integer.parseInt(allele));
            }
        }
        data.put("genes", generefs.toArray(new String[0]));
        data.put("gene_count", genes.size());
        data.put("genesymbols", genes.toArray(new String[0]));
        data.put("alleles", alleles.toArray(new Integer[0]));

        values = (NodeList)xpath.evaluate(record+"SimpleAllele/ProteinChange",
                                          vcv, XPathConstants.NODESET);
        Set<String> proteins = new TreeSet<>();
        for (int i = 0; i < values.getLength(); ++i) {
            Element protein = (Element)values.item(i);
            proteins.add(protein.getTextContent());
        }
        if (!proteins.isEmpty())
            data.put("proteins", proteins.toArray(new String[0]));
        
        if (!funcs.isEmpty())
            data.put("consequences", funcs.toArray(new String[0]));

        List<String> rcv = new ArrayList<>();
        values = (NodeList)xpath.evaluate
            ("./InterpretedRecord/RCVList/RCVAccession/@Accession",
             vcv, XPathConstants.NODESET);
        for (int i = 0; i < values.getLength(); ++i) {
            rcv.add(((Attr)values.item(i)).getValue());
        }
        data.put("RCV", rcv.toArray(new String[0]));
        
        values = (NodeList)xpath.evaluate
            ("./InterpretedRecord/Interpretations/Interpretation",
             vcv, XPathConstants.NODESET);
        List<Map> interps = new ArrayList<>();
        Set<String> conditions = new TreeSet<>();
        int conditionCount = 0;
        for (int i = 0; i < values.getLength(); ++i) {
            Element elm = (Element)values.item(i);

            values = (NodeList) xpath.evaluate
                ("./Description", elm, XPathConstants.NODESET);
            String desc = "";
            if (values.getLength() > 0) {
                desc = ((Element)values.item(0)).getTextContent();
            }
            
            NodeList cites = (NodeList)xpath.evaluate
                ("./Citation/ID[@Source=\"PubMed\"]",
                 elm, XPathConstants.NODESET);
            List<String> pmids = new ArrayList<>();
            for (int j = 0; j < cites.getLength(); ++j)
                pmids.add("PMID:"+((Element)cites.item(j)).getTextContent());
            NodeList traits = (NodeList)xpath.evaluate
                ("./ConditionList/TraitSet/Trait", elm,
                 XPathConstants.NODESET);
            Set<String> xrefs = new TreeSet<>();
            for (int j = 0; j < traits.getLength(); ++j) {
                Element trait = (Element)traits.item(j);

                StringBuilder line = new StringBuilder ();
                for (String sym : genes) {
                    if (line.length() > 0)
                        line.append(",");
                    line.append(sym);
                }
                
                NodeList names = (NodeList) xpath.evaluate
                    ("./Name/ElementValue[@Type=\"Preferred\"]",
                     trait, XPathConstants.NODESET);
                line.append("\t");                
                if (names.getLength() > 0) {
                    line.append(((Element)names.item(0)).getTextContent());
                }
                
                NodeList refs = (NodeList)xpath.evaluate
                    ("./XRef", trait, XPathConstants.NODESET);
                line.append("\t");
                for (int k = 0, xid = 0; k < refs.getLength(); ++k) {
                    Element r = (Element)refs.item(k);
                    String db = r.getAttribute("DB");
                    String id = r.getAttribute("ID");
                    /*
                    System.out.print("^^^ db="+db+" id="+id);
                    for (org.w3c.dom.Node p, n = r; (p = n.getParentNode()) != null; ) {
                        System.out.print(" "+n.getNodeName());
                        n = p;
                    }
                    System.out.println();
                    */
                    String xf = null;
                    switch (db) {
                    case "Orphanet":
                        xf = "ORPHA:"+id;
                        break;
                    case "MedGen":
                        xf = "UMLS:"+id;
                        break;
                    case "OMIM":
                        xf = "OMIM:"+id;
                        break;
                    case "HP":
                    case "Human Phenotype Ontology":
                        xf = id;
                    break;
                    case "Office of Rare Diseases":
                        xf = String.format
                            ("GARD:%1$07d", Integer.parseInt(id));
                        break;
                    }
                    
                    if (xf != null) {
                        if (xid > 0)
                            line.append(",");
                        line.append(xf);
                        xrefs.add(xf);
                        ++xid;
                    }
                }

                line.append("\t");
                NodeList mech = (NodeList)xpath.evaluate
                    ("./AttributeSet/Attribute[@Type=\"disease mechanism\"]",
                     trait, XPathConstants.NODESET);
                if (mech.getLength() > 0) {
                    line.append(((Element)mech.item(0)).getTextContent());
                }

                line.append("\t");
                NodeList tests = (NodeList)xpath.evaluate
                    ("./AttributeSet/XRef", trait, XPathConstants.NODESET);
                for (int k = 0, gtr = 0; k < tests.getLength(); ++k) {
                    Element g = (Element)tests.item(k);
                    String db = g.getAttribute("DB");
                    if (db.indexOf("GTR") > 0) {
                        if (gtr > 0) line.append(",");
                        line.append(g.getAttribute("ID"));
                        ++gtr;
                    }
                }

                if ("Pathogenic".equals(desc)) {
                    out.println(accession+"\t"+desc+"\t"+line);
                }
                
                if ("Disease".equals(trait.getAttribute("Type"))) {
                    ++conditionCount;
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
        data.put("condition_count", conditionCount);

        if (!LITERATURE_ONLY && !interpretations.contains("Pathogenic"))
            return null;

        Entity ent = register (data);
        if (ent != null) {
            logger.info("++++++ "+String.format("%1$6d ", xs.getCount())
                        +data.get("accession")+": genes="+genes
                        +" interps="+interps.size()
                        +" conditions="+conditionCount
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
        try {
            ClinVarVariationParser varp = new ClinVarVariationParser ();
            try (XmlStream xs = new XmlStream
                 (new GZIPInputStream (new FileInputStream (file)),
                  "VariationArchive", varp)) {
                int total = xs.start();
                ds.set(INSTANCES, varp.count);
                updateMeta (ds);
                logger.info("############### "+varp.count+"/"
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
