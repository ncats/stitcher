package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.lang.reflect.Array;

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

public class OMIMUpdateEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(OMIMUpdateEntityFactory.class.getName());
    static final String USER_AGENT =
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11";
    static final String OMIM_API =
        "https://api.omim.org/api/entry?include=all&format=xml";

    int count = 0;
    DocumentBuilder builder;
    XPath xpath;
    Pattern regex = Pattern.compile("\\{([\\d]+):([^\\}]+)");
    
    public OMIMUpdateEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public OMIMUpdateEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OMIMUpdateEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        try {
            builder = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder();
            xpath = XPathFactory.newInstance().newXPath();            
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE,
                       "Can't instantiate new XML document builder!", ex);
        }
    }

    String[] parseReferences (String text, Map<Integer, String> allrefs) {
        Set<String> refs = new TreeSet<>();
        Matcher m = regex.matcher(text);
        while (m.find()) {
            int nr = Integer.parseInt(m.group(1));
            String ref = allrefs.get(nr);
            if (ref == null) {
                logger.log(Level.WARNING, "Bogus reference: "+nr);
            }
            else {
                refs.add(ref);
            }
        }
        return refs.toArray(new String[0]);
    }
    
    boolean update (Entity e, InputStream is) throws Exception {
        Document doc = builder.parse(is);
        Element omim = doc.getDocumentElement();
        String mim = xpath.evaluate("//omim/entryList/entry/mimNumber", omim);
        e.payload("mim", Long.parseLong(mim));
        
        Map<Integer, String> references = new TreeMap<>();
        NodeList nodes = (NodeList)xpath.evaluate
            ("//omim/entryList/entry/referenceList/reference",
             omim, XPathConstants.NODESET);
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element ref = (Element)nodes.item(i);
            try {
                int nr = Integer.parseInt
                    (xpath.evaluate("./referenceNumber", ref));
                String pmid = xpath.evaluate("./pubmedID", ref);
                if (!pmid.equals("")) {
                    references.put(nr, "PMID:"+Long.parseLong(pmid));
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Bogus reference for "+mim, ex);
            }
        }
        logger.info("..."+references.size()+" reference(s)");
        
        nodes = (NodeList)xpath.evaluate
            ("//omim/entryList/entry/textSectionList/textSection",
             omim, XPathConstants.NODESET);
        List<String> sections = new ArrayList<>();
        for (int i = 0; i < nodes.getLength(); ++i) {
            Element sec = (Element)nodes.item(i);
            String name = xpath.evaluate("./textSectionName", sec);
            String text = xpath.evaluate("./textSectionContent", sec);
            String[] refs = parseReferences (text, references);
            logger.info("..."+name+": "+refs.length+" reference(s)!");
            if (refs.length > 0) {
                e.payload("ref:"+name, refs);
            }
            sections.add(name);
        }
        logger.info("...sections: "+sections);
        
        if (!sections.isEmpty()) {
            e.payload("sections", sections.toArray(new String[0]));
        }
        
        return true;
    }

    boolean update (String url, Entity e) throws Exception {
        String id = (String)e.payload("notation");
        if (id != null && id.startsWith("OMIM:")) {
            int pos = id.indexOf('.');
            if (pos < 0) { // don't do variant
                logger.info("++++++ "+String.format("%1$6d...%2$s", count, id));
                if (true) {
                    pos = id.indexOf(':');
                    long nr = Long.parseLong(id.substring(pos+1));
                    URLConnection con = new URL(url+"&mimNumber="+nr)
                        .openConnection();
                    con.setRequestProperty("User-Agent", USER_AGENT);
                    return update (e, con.getInputStream());
                }
                else {
                    return update (e, new FileInputStream ("omim206700.xml"));
                }
            }
        }
        return false;
    }
    
    public int update (String apiKey) throws Exception {
        count = 0;
        final String base = OMIM_API + "&"+apiKey;
        final Random rand = new Random ();
        final AtomicInteger errors = new AtomicInteger ();
        entities ("S_OMIM", e -> {
                try {
                    if (update (base, e)) {
                        if (++count % 20 == 0) {
                            Thread.currentThread().sleep
                                (1000 + rand.nextInt(5000));
                        }
                    }
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't update entity "
                               +e.payload("notation")+"!", ex);
                    if (errors.incrementAndGet() >= 5)
                        throw new RuntimeException
                            ("Bailing out after "+errors.get()
                             +" failed attempts!");
                }
            });
        return count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+OMIMUpdateEntityFactory.class.getName()
                        +" DBDIR apiKey=XXXXXX");
            System.exit(1);
        }
        
        try (OMIMUpdateEntityFactory omim =
             new OMIMUpdateEntityFactory (argv[0])) {
            int count = omim.update(argv[1]);
            logger.info("### "+count+" OMIM entities updated from API!");
        }
    }
}
