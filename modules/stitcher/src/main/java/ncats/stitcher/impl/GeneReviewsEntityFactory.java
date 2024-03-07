package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
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

import javax.xml.parsers.*;
import javax.xml.xpath.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Attr;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/**
 * sbt stitcher/"runMain ncats.stitcher.impl.GeneReviewsEntityFactory DB gene_NBK1116"
 */

public class GeneReviewsEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(GeneReviewsEntityFactory.class.getName());

    static final Set<String> DONTINCLUDE = new HashSet<>();
    static {
        DONTINCLUDE.add("Not applicable");
    }

    static public final String SOURCE_NAME = "GENEREVIEWS";    
    DocumentBuilder builder;
    final XPath xpath = XPathFactory.newInstance().newXPath();
    final ObjectMapper mapper = new ObjectMapper ();
    final SimpleDateFormat sdf = new SimpleDateFormat ("MM/dd/yyy");
    final Map<Entity, String[]> xrefs = new HashMap<>();
    
    public GeneReviewsEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public GeneReviewsEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public GeneReviewsEntityFactory (File dir) throws IOException {
        super (dir);
    }
    
    @Override
    protected void init () {
        super.init();
        setIdField ("id");
        setNameField ("title");
        add (I_GENE, "genes")
            .add(N_Name, "disease")
            .add(N_Name, "synonyms")
            ;
        
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();

            String FEATURE = "http://apache.org/xml/features/disallow-doctype-decl";
            dbf.setFeature(FEATURE, true);
            dbf.setXIncludeAware(false);

            builder = dbf.newDocumentBuilder();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't construct document factory!", ex);
        }
    }

    static String[] getText (Node node) {
        return getText (node, DONTINCLUDE);
    }
    
    static String[] getText (Node node, Set<String> dontinclude) {
        List<String> texts = new ArrayList<>();
        getText (texts, node, dontinclude);
        return texts.toArray(new String[0]);
    }

    static void getText (List<String> texts,
                         Node node, Set<String> dontinclude) {
        if (Node.TEXT_NODE == node.getNodeType()) {
            String s = node.getTextContent().trim();
            if (!s.equals("") && (dontinclude == null
                                  || !dontinclude.contains(s)))
                texts.add(s);
        }
        
        for (Node child = node.getFirstChild(); child != null; ) {
            getText (texts, child, dontinclude);
            child = child.getNextSibling();
        }
    }

    Map<String, Object> parseAuthor (Element contrib) throws Exception {
        Map<String, Object> author = new LinkedHashMap<>();
        
        if ("author".equals(contrib.getAttribute("contrib-type"))) {
            author.put("lastname", xpath.evaluate("./name/surname", contrib));
            author.put("firstname",
                       xpath.evaluate("./name/given-names", contrib));
            author.put("degrees", xpath.evaluate("./degrees", contrib));
            NodeList nodes = (NodeList)xpath.evaluate
                ("./xref[@ref-type=\"aff\"]", contrib, XPathConstants.NODESET);
            if (nodes.getLength() > 0) {
                author.put("affiliation",
                           ((Element)nodes.item(0)).getAttribute("rid"));
            }
            else {
                nodes = (NodeList)xpath.evaluate
                    ("./aff", contrib, XPathConstants.NODESET);
                if (nodes.getLength() > 0) {
                    author.put("affiliation", getText (nodes.item(0)));
                }
            }
            author.put("email", xpath.evaluate("./email", contrib));
        }
        return author;
    }
    
    Map[] parseContrib (Element root) throws Exception {
        NodeList nodes =(NodeList)xpath.evaluate
            ("//book-part-wrapper/book-part/book-part-meta/aff",
             root, XPathConstants.NODESET);
        Map<String, Object> affiliations = new HashMap<>();
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element aff = (Element)nodes.item(i);
            affiliations.put(aff.getAttribute("id"), getText (aff));
        }

        nodes = (NodeList)xpath.evaluate
            ("//book-part-wrapper/book-part/book-part-meta/"
             +"contrib-group/contrib", root, XPathConstants.NODESET);
        List<Map> authors = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element node = (Element)nodes.item(i);
            Map<String, Object> author = parseAuthor (node);
            if (!author.isEmpty()) {
                if (!affiliations.isEmpty()) {
                    Object aff = author.get("affiliation");
                    if (affiliations.containsKey((String)aff))
                        author.put("affiliation", affiliations.get(aff));
                }
                if (false) {
                    String json = mapper.writerWithDefaultPrettyPrinter()
                        .writeValueAsString(author);
                    logger.info("..."+json);
                }
                authors.add(author);
            }
        }
        return authors.toArray(new Map[0]);
    }

    Map<String, Object> parseHistory (Map<String, Object> hist,
                                      Element root) throws Exception {
        if (hist == null)
            hist = new LinkedHashMap<>();
        
        NodeList nodes =(NodeList)xpath.evaluate
            ("//book-part-wrapper/book-part/book-part-meta/pub-history/date",
             root, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element node = (Element)nodes.item(i);
            try {
                String date = String.format
                    ("%02d/%02d/%04d",
                     Integer.parseInt(xpath.evaluate("./month", node)),
                     Integer.parseInt(xpath.evaluate("./day", node)),
                     Integer.parseInt(xpath.evaluate("./year", node)));
                String type = node.getAttribute("date-type");
                hist.put(type, date);
                logger.info("..."+type+": "+date);
            }
            catch (NumberFormatException ex) {
                logger.warning("Bad date format");
            }
        }
        return hist;
    }
    
    Entity registerChapter (File file) throws Exception {
        Map<String, Object> data = new LinkedHashMap<>();
        Document doc = builder.parse(new FileInputStream (file));
        Element root = doc.getDocumentElement();

        String id = root.getAttribute("id");
        data.put("id", id);
        String title = xpath.evaluate("//book-part-wrapper/book-part/"
                                      +"book-part-meta/title-group/title",
                                      root);
        data.put("title", title);
        NodeList nodes = (NodeList)xpath.evaluate
            ("//book-part-wrapper/book-part/book-part-meta/kwd-group",
             root, XPathConstants.NODESET);

        String[] include = null;
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element node = (Element)nodes.item(i);
            String[] texts = getText (node);
            if (texts.length > 0) {
                switch (node.getAttribute("kwd-group-type")) {
                case "Synonyms": case "Synonym":
                    data.put("synonyms", texts);
                    break;
                case "GeneSymbol":
                    data.put("genes", texts);
                    break;
                case "DiseaseName":
                    data.put("disease", texts);
                    break;
                case "ProteinName":
                    data.put("protein", texts);
                    break;
                case "Include":
                    // this is the parent disease of entries listed here
                    include = texts;
                    break;
                }
            }
        }
        parseHistory (data, root);

        logger.info("#### "+data);
        Entity ent = register (data);
        logger.info("+++++ "+id+": "+title+" ("+ent.getId()+")");
        if (include != null) {
            xrefs.put(ent, include);
        }
        
        Map[] authors = parseContrib (root);
        for (int i = 0; i < authors.length; ++i) {
            Map<String, Object> props = new LinkedHashMap<>();
            props.put(ID, "author-"+(i+1));
            props.put(SOURCE, source.getKey());
            ent.addIfAbsent("AUTHOR", props, (Map<String, Object>)authors[i]);
        }
        
        return ent;
    }

    @Override
    public DataSource register (File dir) {
        DataSource ds = getDataSourceFactory().register(SOURCE_NAME);
        setDataSource (ds);
        ds.set(Props.URI, dir.toURI().toString());

        logger.info("############## registering entities for "+dir);
        try {

            Document doc = builder.parse
                (new FileInputStream (new File (dir, "TOC.nxml")));
            Element root = doc.getDocumentElement();
            NodeList values = (NodeList)xpath.evaluate
                ("//book-part-wrapper/toc/toc-entry/@id",
                 root, XPathConstants.NODESET);
            int count = 0;
            for (int i = 0; i < values.getLength(); ++i) {
                String id = ((Attr)values.item(i)).getValue();
                // skip toc_
                String fname = id.startsWith("toc_") ? id.substring(4) : id;
                File file = new File (dir, fname+".nxml");
                if (file.exists()) {
                    Entity e = registerChapter (file);
                    if (e != null) {
                        ++count;
                        if (false && count > 100) break;
                    }
                }
            }
            ds.set(INSTANCES, count);
            updateMeta (ds);
            logger.info("############## "+count+" entities registered!");
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse "+dir, ex);
        }
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+GeneReviewsEntityFactory.class.getName()
                        +" DB GENE_DIR...");
            System.exit(1);
        }

        File dir = new File (argv[1]);
        if (!dir.isDirectory()) {
            logger.log(Level.SEVERE, dir+": not a directory!");
            System.exit(1);
        }

        try (GeneReviewsEntityFactory gv =
             new GeneReviewsEntityFactory (argv[0])) {
            gv.register(dir);
        }
    }
}
