package ncats.stitcher.impl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.regex.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import javax.xml.stream.*;
import javax.xml.stream.events.*;
import javax.xml.namespace.QName;
import javax.xml.parsers.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class OrphanetPrevalenceEntityFactory extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetPrevalenceEntityFactory.class.getName());

    static final QName Id = new QName (NS, "id");
    static final QName PrevalenceList = new QName (NS, "PrevalenceList");
    static final QName Prevalence = new QName (NS, "Prevalence");
    static final QName Source = new QName (NS, "Source");
    static final QName ValMoy = new QName (NS, "ValMoy");
    static final QName PrevalenceType = new QName (NS, "PrevalenceType");
    static final QName PrevalenceClass = new QName (NS, "PrevalenceClass");
    static final QName PrevalenceQualification =
        new QName (NS, "PrevalenceQualification");
    static final QName PrevalenceGeographic =
        new QName (NS, "PrevalenceGeographic");
    static final QName PrevalenceValidationStatus =
        new QName (NS, "PrevalenceValidationStatus");

    static class Prevalence {
        public Integer id;
        public Set<Long> pmids = new TreeSet<>();
        public String type;
        public String qualification;
        public String pclass;
        public Double valmoy;
        public String geo;
        public String status;

        Map<String, Object> toMap () {
            Map<String, Object> data = new LinkedHashMap<>();
            if (!pmids.isEmpty())
                data.put("Source", pmids.stream().map(p -> "PMID:"+p)
                         .toArray(String[]::new));
            if (type != null)
                data.put("PrevalenceType", type);
            if (qualification != null)
                data.put("PrevalenceQualification", qualification);
            if (pclass != null)
                data.put("PrevalenceClass", pclass);
            if (valmoy != null)
                data.put("ValMoy", valmoy);
            if (geo != null)
                data.put("PrevalenceGeographic", geo);
            if (status != null)
                data.put("PrevalenceValidationStatus", status);
            return data;
        }
    }
    
    static class PrevalenceDisorder extends Disorder {
        public LinkedList<Prevalence> prevalences = new LinkedList<>();
    }
    
    public OrphanetPrevalenceEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetPrevalenceEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetPrevalenceEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected int register (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        Map<String, Object> props = new HashMap<>();
        props.put(SOURCE, source.getKey());

        Pattern re = Pattern.compile("([\\d]+)\\[([^\\]]+)");
        int count = 0;
        ObjectMapper mapper = new ObjectMapper ();
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        LinkedList<PrevalenceDisorder> dstack = new LinkedList<>();

        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();                
                QName qn = se.getName();
                if (Disorder.equals(qn)) {
                    PrevalenceDisorder d = new PrevalenceDisorder ();
                    dstack.push(d);
                }
                else if (PrevalenceList.equals(qn)) {
                }
                else if (Prevalence.equals(qn)) {
                    Prevalence p = new Prevalence ();
                    Attribute attr = se.getAttributeByName(Id);
                    if (attr != null)
                        p.id = Integer.parseInt(attr.getValue());
                    dstack.peek().prevalences.push(p);
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString();

                PrevalenceDisorder pd = dstack.peek();
                QName qn = se.getName();
                QName pn = null;
                if (parent != null)
                    pn = parent.getName();
                
                //System.out.println(qn);
                if (Disorder.equals(qn)) {
                    pd = dstack.pop();
                    /*
                    System.out.println(mapper.writerWithDefaultPrettyPrinter()
                                       .writeValueAsString(pd));
                    */
                    Entity[] ents = getEntities (pd);
                    if (ents.length > 0) {
                        for (Prevalence p : pd.prevalences) {
                            props.put(ID, p.id);
                            for (Entity e : ents)
                                e.addIfAbsent("PREVALENCE", props, p.toMap());
                        }
                        ++count;
                        logger.info
                            ("+++++ "+String.format("%1$5d: ", count)
                             +pd.orphaNumber+": "+pd.name+"..."
                             +pd.prevalences.size()+" prevalence(s)");
                    }
                }
                else if (OrphaNumber.equals(qn)) {
                    if (Disorder.equals(pn))
                        pd.orphaNumber = Integer.parseInt(value);
                }
                else if (ExpertLink.equals(qn)) {
                    pd.link = value;
                }
                else if (Name.equals(qn)) {
                    if (Disorder.equals(pn)) {
                        pd.name = value;
                    }
                    else if (DisorderType.equals(pn)) {
                        pd.type = value;
                    }
                    else if (PrevalenceType.equals(pn)) {
                        pd.prevalences.peek().type = value;
                    }
                    else if (PrevalenceValidationStatus.equals(pn)) {
                        pd.prevalences.peek().status = value;
                    }
                    else if (PrevalenceGeographic.equals(pn)) {
                        pd.prevalences.peek().geo = value;
                    }
                    else if (PrevalenceQualification.equals(pn)) {
                        pd.prevalences.peek().qualification = value;
                    }
                    else if (PrevalenceClass.equals(pn)) {
                        pd.prevalences.peek().pclass = value;
                    }
                }
                else if (ValMoy.equals(qn)) {
                    if (!value.equals(""))
                        pd.prevalences.peek().valmoy =
                            Double.parseDouble(value);
                }
                else if (Source.equals(qn)) {
                    for (String tok : value.split("_")) {
                        Matcher m = re.matcher(tok);
                        while (m.find()) {
                            String source = m.group(2);
                            if ("PMID".equals(source)) {
                                String pmid = m.group(1);
                                try {
                                    pd.prevalences.peek().pmids.add
                                        (Long.parseLong(pmid));
                                }
                                catch (NumberFormatException ex) {
                                    logger.warning("Bogus PMID: "+pmid);
                                }
                            }
                        }
                    }
                }
            }                
            else if (ev.isCharacters()) {
                buf.append(ev.asCharacters().getData());
            }
        }
        events.close();

        return count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "
                        +OrphanetPrevalenceEntityFactory.class.getName()
                        +" DBDIR XML");
            System.exit(1);
        }
        
        try (OrphanetPrevalenceEntityFactory orph =
             new OrphanetPrevalenceEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                DataSource ds = orph.register(file);
            }
        }
    }
}
