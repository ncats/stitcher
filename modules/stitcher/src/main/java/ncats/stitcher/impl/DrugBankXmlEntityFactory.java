package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.Base64;
import java.net.URL;

import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;
import org.w3c.dom.*;


import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class DrugBankXmlEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(DrugBankXmlEntityFactory.class.getName());

    static final String NS = "http://www.drugbank.ca";
    static final QName Root = new QName (NS, "drugbank");
    static final QName Drug = new QName (NS, "drug");
    static final QName DrugBankId = new QName (NS, "drugbank-id");
    static final QName Synonyms = new QName (NS, "synonyms");
    static final QName Synonym = new QName (NS, "synonym");
    static final QName Property = new QName (NS, "property");
    static final QName Groups = new QName (NS, "groups");
    static final QName Group = new QName (NS, "group");
    static final QName Name = new QName (NS, "name");
    static final QName Primary = new QName ("primary"); // attribute
    
    static final QName[] TAGS = {
        new QName (NS, "indication"),
        new QName (NS, "unii"),
        new QName (NS, "mechanism-of-action"),
        new QName (NS, "metabolism"),
        new QName (NS, "pharmacodynamics"),
        new QName (NS, "route-of-elimination"),
        new QName (NS, "toxicity"),
        new QName (NS, "clearance"),
        new QName (NS, "description"),
        new QName (NS, "half-life")
    };

    static final String[] PROPS = new String[]{
        "InChIKey",
        "SMILES"
    };

    int count;
    
    public DrugBankXmlEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public DrugBankXmlEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public DrugBankXmlEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("drugbank-id");
        setNameField ("name");
        setStrucField ("SMILES");
        setEventParser(ncats.stitcher.calculators.events.DrugBankXmlEventParser.class.getCanonicalName());
        //add (N_Name, "synonyms");
        add (N_Name, "name");
        //add (I_CAS, "cas-number");
        add (I_UNII, "unii");
        //add (H_InChIKey, "InChIKey");
        //add (I_CODE, "drugbank-id-others");
        add (T_Keyword, "groups");
    }

    void register (Map<String, Object> payload, int total) {
        System.out.println("+++++ "+payload.get("drugbank-id")+" "
                           +(count+1)+"/"+total+" +++++");
        register (payload);
        ++count;
    }

    void parseXml (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        Map<String, Object> payload = new TreeMap<>();
        Map<String, Object> props = new TreeMap<>();
        StringBuilder snippet = new StringBuilder ();
        int total = 0;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                StartElement parent = (StartElement) stack.peek();
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();
                QName name = se.getName();
                if (Drug.equals(name) && Root.equals(parent.getName())) {
                    payload.clear();
                    snippet.setLength(0);
                }
                else if (Property.equals(name)) {
                    props.clear();
                }
                snippet.append("<"+name.getLocalPart()+">");
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                QName name = se.getName();
                snippet.append("</"+name.getLocalPart()+">");

                if (Root.equals(name)) {
                    // we're done
                }
                else if (DrugBankId.equals(name)
                         && Drug.equals(parent.getName())
                         && stack.size() == 2) {
                    Attribute attr = se.getAttributeByName(Primary);
                    if (attr != null
                        && "true".equalsIgnoreCase(attr.getValue())) {
                        payload.put(name.getLocalPart(), buf.toString());
                    }
                    else {
                        String key = name.getLocalPart()+"-others";
                        Object val = payload.get(key);
                        payload.put(key, Util.merge(val, buf.toString()));
                    }
                }
                else if (Name.equals(name)
                         && Drug.equals(parent.getName())
                         && stack.size() == 2) {
                    payload.put(name.getLocalPart(), buf.toString());
                }
                else if (Synonym.equals(name)
                         && Synonyms.equals(parent.getName())
                         // make sure we only grab drug synonyms
                         // and not target/enzymes/etc
                         && stack.size() == 3) {
                    /*
                    for (XMLEvent e : stack)
                        System.out.println(e.asStartElement().getName());
                    */
                    payload.put("synonyms",
                                Util.merge(payload.get("synonyms"),
                                           buf.toString()));
                }
                else if (Group.equals(name)
                         && Groups.equals(parent.getName())) {
                    payload.put("groups",
                                Util.merge(payload.get("groups"),
                                           buf.toString()));
                }
                else if (Drug.equals(name) && Root.equals(parent.getName())) {
                    String xml = Util.encode64(snippet.toString(), true);
                    payload.put("xml", xml);
                    register (payload, ++total);
                    //System.out.println(payload);
                    //System.out.println(Util.decode64(xml, true));
                }
                else if (Property.equals(name)) {
                    for (String p : PROPS) {
                        if (p.equals(props.get("kind")))
                            payload.put(p, props.get("value"));
                    }
                }
                else if (Property.equals(parent.getName())) {
                    props.put(name.getLocalPart(), buf.toString());
                }
                else {
                    for (QName q : TAGS) {
                        if (q.equals(name) && buf.length() > 0) {
                            payload.put(name.getLocalPart(), buf.toString());
                            break;
                        }
                    }
                }
                buf.setLength(0);
            }
            else if (ev.isCharacters()) {
                Characters chars = ev.asCharacters();
                String s = chars.getData();
                for (int i = 0; i < s.length(); ++i) {
                    char ch = s.charAt(i);
                    switch (ch) {
                    case '&':
                        snippet.append("&amp;");
                        break;
                    case '<':
                        snippet.append("&lt;");
                        break;
                    case '>':
                        snippet.append("&gt;");
                        break;
                    default:
                        if (ch > 0x7e)
                            snippet.append("&#"+((int)ch)+";");
                        else
                            snippet.append(ch);
                    }
                    buf.append(ch);
                }
            }
        }
        events.close(); 
    }

    void parseZip (InputStream is) throws IOException {
        ZipInputStream zis = new ZipInputStream (is);
        try {
            ZipEntry ze = zis.getNextEntry();
            if (ze != null && !ze.isDirectory()) {
                String name = ze.getName();
                if (name.endsWith(".xml")) {
                    logger.info("parsing entry \""+name+"\"...");
                    parseXml (zis);
                }
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse zip entry", ex);
        }
    }
    
    @Override
    public int register (InputStream is) throws IOException {
        count = 0;

        BufferedInputStream bis = new BufferedInputStream (is);
        bis.mark(1024);
        byte[] magic = new byte[4];
        int nb = bis.read(magic, 0, magic.length);
        if (nb == magic.length) {
            bis.reset(); // reset the buffer
            // check for zip magic 0x04034b50 as little endian
            try {
                if ((magic[0] & 0xff) == 0x50 &&
                    (magic[1] & 0xff) == 0x4b &&
                    (magic[2] & 0xff) == 0x03 &&
                    (magic[3] & 0xff) == 0x04) {
                    // zip file
                    parseZip (bis);
                }
                else { // assume xml
                    parseXml (bis);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't parse XML stream!", ex);
            }
        }
        else {
            logger.warning("File contains "+nb+" byte(s); nothing parsed!");
        }
        bis.close();
        
        return count;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +DrugBankXmlEntityFactory.class.getName()
                               +" DBDIR [cache=DIR] FILE...");
            System.exit(1);
        }

        DrugBankXmlEntityFactory mef = new DrugBankXmlEntityFactory (argv[0]);
        String sourceName = null;
        try {
            for (int i = 1; i < argv.length; ++i) {
                int pos = argv[i].indexOf('=');
                if (pos > 0) {
                    String name = argv[i].substring(0, pos);
                    if (name.equalsIgnoreCase("cache")) {
                        mef.setCache(argv[i].substring(pos+1));
                    }
                    else if (name.equalsIgnoreCase("name")) {
                        sourceName = argv[i].substring(pos+1);
                        System.out.println(sourceName);
                    }
                    else {
                        logger.warning("** Unknown parameter \""+name+"\"!");
                    }
                }
                else {
                    File file = new File(argv[i]);

                    if(sourceName != null){
                        mef.register(sourceName, file);
                    }
                    else {
                        mef.register(file.getName(), file);
                    }
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }
}

