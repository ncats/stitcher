package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;
import org.w3c.dom.*;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * Genetics home reference
 * https://ghr.nlm.nih.gov/download/ghr-summaries.xml
 */
public class GHREntityFactory extends EntityRegistry {
    static final String GHR_URL =
        "https://ghr.nlm.nih.gov/download/ghr-summaries.xml";
    static final Logger logger =
        Logger.getLogger(GHREntityFactory.class.getName());

    static final String NS =
        "https://ghr.nlm.nih.gov/download/ghr-summaries-20170124.xsd";
    static final QName Root = new QName (NS, "summaries");
    static final QName Name = new QName (NS, "name");
    static final QName Page = new QName (NS, "ghr-page");
    static final QName Html = new QName (NS, "html");
    static final QName HealthConditionSummary =
        new QName (NS, "health-condition-summary");
    static final QName Synonym = new QName (NS, "synonym");
    static final QName SynonymList = new QName (NS, "synonym-list");
    static final QName Db = new QName (NS, "db");
    static final QName Key = new QName (NS, "key");
    static final QName DbKey = new QName (NS, "db-key");
    static final QName DbKeyList = new QName (NS, "db-key-list");
    static final QName Reviewed = new QName (NS, "reviewed");
    static final QName Published = new QName (NS, "published");
    static final QName RelatedChromosome = new QName (NS, "related-chromosome");
    static final QName RelatedGene = new QName (NS, "related-gene");
    static final QName RelatedGeneList = new QName (NS, "related-gene-list");
    static final QName GeneSymbol = new QName (NS, "gene-symbol");
    static final QName TextRole = new QName (NS, "text-role");
    static final QName HtmlP = new QName ("http://www.w3.org/1999/xhtml", "p");
    static final QName Text = new QName (NS, "text");

    public GHREntityFactory (String dir) throws IOException {
        super (dir);
    }

    public GHREntityFactory (File dir) throws IOException {
        super (dir);
    }

    public GHREntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setNameField ("name");
        setIdField ("name");
        add (N_Name, "name")
            .add(N_Name, "synonyms")
            .add(I_GENE, "genes")
            .add(I_CODE, "xrefs")
            ;
    }
    
    public DataSource register (InputStream is) throws Exception {
        DataSource ds = getDataSourceFactory().register("GHR");
        setDataSource (ds);
        
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        Map<String, Object> data = new TreeMap<>();
        String db = null, key = null, textRole = null;
        List<String> text = new ArrayList<>();
        List<String> xrefs = new ArrayList<>();
        List<String> syns = new ArrayList<>();
        int count = 0;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                StartElement parent = (StartElement) stack.peek();
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();                
                QName name = se.getName();
                if (HealthConditionSummary.equals(name)) {
                    ++count;
                    data.clear();
                }
                else if (DbKey.equals(name)) {
                    db = null;
                    key = null;
                }
                else if (Text.equals(name))
                    text.clear();
                else if (TextRole.equals(name))
                    textRole = null;
                else if (DbKeyList.equals(name))
                    xrefs.clear();
                else if (SynonymList.equals(name))
                    syns.clear();
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString();
                
                QName name = se.getName();
                //System.out.println(name);
                
                if (Root.equals(name)) {
                }
                else if (HealthConditionSummary.equals(name)) {
                    // register current record
                    ncats.stitcher.Entity e = register (data);
                    logger.info(count+": "+data.get("name")+" ("+e.getId()+")");
                }
                else if (HealthConditionSummary.equals(parent.getName())) {
                    if (Name.equals(name)) {
                        data.put("name", value);
                    }
                    else if (Page.equals(name)) {
                        data.put("ghr-page", value);
                    }
                    else if (Reviewed.equals(name))
                        data.put("reviewed", value);
                    else if (Published.equals(name))
                        data.put("published", value);
                }
                else if (Synonym.equals(name)
                         && SynonymList.equals(parent.getName())) {
                    syns.add(value);
                }
                else if (SynonymList.equals(name)) {
                    data.put("synonyms", syns.toArray(new String[0]));
                }
                else if (HtmlP.equals(name)) {
                    text.add(value);
                }
                else if (Name.equals(name)
                         && RelatedChromosome.equals(parent.getName())) {
                    data.put("chromosome", value);
                }
                else if (GeneSymbol.equals(name)) {
                    Object old = data.get("genes");
                    data.put("genes", old != null
                             ? Util.merge(old, value) : value);
                }
                else if (TextRole.equals(name)) {
                    textRole = value;
                }
                else if (Text.equals(name)) {
                    data.put(textRole, text.toArray(new String[0]));
                }
                else if (Db.equals(name)) {
                    db = buf.toString().replaceAll("\\s", "_").toUpperCase();
                }
                else if (Key.equals(name)) {
                    key = value;
                }
                else if (DbKey.equals(name)
                         && DbKeyList.equals(parent.getName())) {
                    xrefs.add(db+":"+key);
                }
                else if (DbKeyList.equals(name)) {
                    data.put("xrefs", xrefs.toArray(new String[0]));
                }
            }
            else if (ev.isCharacters()) {
                buf.append(ev.asCharacters().getData());
            }
        }
        events.close();
        
        logger.info(count+" entities registered!");
        ds.set(INSTANCES, count);
        updateMeta (ds);
        
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "+GHREntityFactory.class.getName()
                        +" DBDIR [FILE|URL]\nIf not specified, "+GHR_URL
                        +"is used!");
            System.exit(1);
        }

        URL xml = null;
        if (argv.length > 1) {
            if (argv[1].startsWith("http"))
                xml = new URL (argv[1]);
            else {
                File file = new File (argv[1]);
                xml = file.toURL();
            }
        }
        else {
            xml = new URL (GHR_URL);
        }
        logger.info("Registering "+xml+"...");
        try (GHREntityFactory ghr = new GHREntityFactory (argv[0])) {
            DataSource ds = ghr.register(xml.openStream());
            ds.set("url", xml.toString());
        }
    }
}
