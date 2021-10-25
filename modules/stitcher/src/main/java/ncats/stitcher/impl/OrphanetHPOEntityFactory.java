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

public class OrphanetHPOEntityFactory extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetHPOEntityFactory.class.getName());

    static final QName HPODisorderAssociationList =
        new QName (NS, "HPODisorderAssociationList");
    static final QName HPODisorderAssociation =
        new QName (NS, "HPODisorderAssociation");
    static final QName HPO = new QName (NS, "HPO");
    static final QName HPOFrequency = new QName (NS, "HPOFrequency");
    static final QName HPOId = new QName (NS, "HPOId");
    static final QName HPOTerm = new QName (NS, "HPOTerm");

    static class HPO {
        public String id;
        public String term;
        public String frequency;
        public List<Entity> entities = new ArrayList<>();
    }
    
    static class HPODisorder extends Disorder {
        public LinkedList<HPO> phenotypes = new LinkedList<>();
    }

    public OrphanetHPOEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetHPOEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetHPOEntityFactory (File dir) throws IOException {
        super (dir);
    }

    Entity[] getHPOEntities (HPO hpo) {
        if (hpo.entities.isEmpty()) {
            for (Iterator<Entity> iter = find ("notation", hpo.id);
                 iter.hasNext(); ) {
                hpo.entities.add(iter.next());
            }
        }
        return hpo.entities.toArray(new Entity[0]);
    }
    
    @Override
    protected int register (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        Map<String, Object> props = new HashMap<>();
        props.put(SOURCE, source.getKey());

        int count = 0;
        ObjectMapper mapper = new ObjectMapper ();
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        LinkedList<HPODisorder> dstack = new LinkedList<>();
        
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();                
                QName qn = se.getName();
                if (Disorder.equals(qn)) {
                    HPODisorder d = new HPODisorder ();
                    dstack.push(d);
                }
                else if (HPODisorderAssociation.equals(qn)) {
                    dstack.peek().phenotypes.push(new HPO ());
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString();

                HPODisorder hd = dstack.peek();
                QName qn = se.getName();
                QName pn = null;
                if (parent != null)
                    pn = parent.getName();
                
                //System.out.println(qn);
                if (Disorder.equals(qn)) {
                    hd = dstack.pop();
                    /*
                    System.out.println(mapper.writerWithDefaultPrettyPrinter()
                                       .writeValueAsString(hd));
                    */
                    Entity[] ents = getEntities (hd);
                    if (ents.length > 0) {
                        for (HPO p : hd.phenotypes) {
                            getHPOEntities (p);
                            if (p.frequency != null)
                                props.put("frequency", p.frequency);
                            else
                                props.remove("frequency");
                            
                            for (Entity e : ents)
                                for (Entity h : p.entities)
                                    e.stitch(h, R_hasPhenotype, p.id, props);
                        }
                        ++count;
                        logger.info
                            ("+++++ "+String.format("%1$5d: ", count)
                             +hd.orphaNumber+": "+hd.name+"..."
                             +hd.phenotypes.size()+" phenotype(s)");
                    }
                }
                else if (OrphaNumber.equals(qn) || OrphaCode.equals(qn)) {
                    if (Disorder.equals(pn))
                        hd.orphaNumber = Integer.parseInt(value);
                }
                else if (HPOId.equals(qn)) {
                    hd.phenotypes.peek().id = value;
                }
                else if (HPOTerm.equals(qn)) {
                    hd.phenotypes.peek().term = value;
                }
                else if (Name.equals(qn)) {
                    if (Disorder.equals(pn)) {
                        hd.name = value;
                    }
                    else if (HPOFrequency.equals(pn)) {
                        hd.phenotypes.peek().frequency = value;
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
                        +OrphanetHPOEntityFactory.class.getName()
                        +" DBDIR XML");
            System.exit(1);
        }
        
        try (OrphanetHPOEntityFactory orph =
             new OrphanetHPOEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                DataSource ds = orph.register(file);
            }
        }
    }
}
