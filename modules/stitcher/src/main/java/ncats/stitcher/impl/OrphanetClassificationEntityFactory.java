package ncats.stitcher.impl;

import java.io.*;
import java.net.*;
import java.util.*;
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

/*
 * this class assumes all orphanet entities have been loaded 
 *   via OntEntityFactory
 * https://github.com/Orphanet/Orphadata.org.git
 */ 
public class OrphanetClassificationEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(OrphanetClassificationEntityFactory.class.getName());

    static final String NS = "";
    static final QName DisorderList = new QName (NS, "DisorderList");
    static final QName Disorder = new QName (NS, "Disorder");
    static final QName OrphaNumber = new QName (NS, "OrphaNumber");
    static final QName ClassificationNodeList =
        new QName (NS, "ClassificationNodeList");
    static final QName ClassificationNode =
        new QName (NS, "ClassificationNode");
    static final QName ClassificationNodeChildList =
        new QName (NS, "ClassificationNodeChildList");
    static final QName Name = new QName (NS, "Name");

    static class Disorder {
        Disorder parent;
        Integer orphaNumber;
        String name;
        Entity entity;
        List<Disorder> children = new ArrayList<>();

        void add (Disorder d) {
            d.parent = this;
            children.add(d);
        }
    }

    public OrphanetClassificationEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetClassificationEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetClassificationEntityFactory (File dir) throws IOException {
        super (dir);
    }
        
    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null || count == 0) {
            setDataSource (ds);
            try (InputStream is = new FileInputStream (file)) {
                count = register (is);
                ds.set(INSTANCES, count);
                updateMeta (ds);
            }
            catch (Exception ex) {
                throw new IOException (ex);
            }
        }
        else {
            logger.info("### Data source "+ds.getName()+" ("+ds.getKey()+") "
                        +"is already registered with "+count+" entities!");
        }
        return ds;        
    }

    Entity instrument (Disorder d) {
        if (d.entity == null) {
            Iterator<Entity> iter = find ("notation", "ORPHA:"+d.orphaNumber);
            if (iter.hasNext()) {
                d.entity = iter.next();
                if (d.parent != null) {
                    Entity p = instrument (d.parent);
                    if (p != null) {
                        Map<String, Object> r = new LinkedHashMap<>();
                        if (source != null)
                            r.put(SOURCE, source.getName());
                        
                        d.entity.stitch(p, R_subClassOf,
                                        "ORPHA:"+d.parent.orphaNumber, r);
                        //System.out.println(d.entity.getId()+" -> "+p.getId());
                    }
                }
            }
            else {
                logger.warning("Unable to find entity for ORPHA:"+d.orphaNumber
                               +" "+d.name);
            }
        }
        return d.entity;
    }
    
    void register (Disorder d) {
        for (Disorder p = d.parent; p != null; p = p.parent) {
            System.out.print("..");
        }
        System.out.println(d.orphaNumber+" "+d.name);

        instrument (d);
        for (Disorder c : d.children)
            register (c);
    }

    protected int register (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        LinkedList<Disorder> dstack = new LinkedList<>();
        List<Disorder> disorders = new ArrayList<>();
        int count = 0;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                StartElement parent = (StartElement) stack.peek();
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();                
                QName qn = se.getName();
                if (qn.equals(Disorder)) {
                    Disorder p = dstack.peek();
                    Disorder d = new Disorder ();
                    dstack.push(d);
                    if (parent.getName().equals(ClassificationNode))
                        p.add(d);
                    else if (parent.getName().equals(DisorderList))
                        disorders.add(d);
                    ++count;
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString();
                
                QName qn = se.getName();
                //System.out.println(qn);
                if (OrphaNumber.equals(qn)) {
                    dstack.peek().orphaNumber = Integer.parseInt(value);
                }
                else if (Name.equals(qn)) {
                    dstack.peek().name = value;
                }
                else if (ClassificationNode.equals(qn)) {
                    Disorder d = dstack.pop();
                }
            }                
            else if (ev.isCharacters()) {
                buf.append(ev.asCharacters().getData());
            }
        }
        events.close();

        for (Disorder d : disorders)
            register (d);

        return count;
    }

    public static void register (OrphanetClassificationEntityFactory orph,
                                 File file) throws Exception {
        if (file.isDirectory()) {
            File[] files = file.listFiles
                ((f) -> f.isFile() && f.getName().endsWith(".xml"));
            for (File f : files)
                register (orph, f);
        }
        else {
            try {
                logger.info("### registering "+file+"...");
                DataSource ds = orph.register(file);
                logger.info(file+": "+ds.get(INSTANCES));
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't register "+file, ex);
            }
        }
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "
                        +OrphanetClassificationEntityFactory.class.getName()
                        +" DBDIR XML...");
            System.exit(1);
        }
        
        try (OrphanetClassificationEntityFactory orph =
             new OrphanetClassificationEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                register (orph, file);
            }
        }
    }
}
