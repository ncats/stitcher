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
public class OrphanetClassificationEntityFactory extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetClassificationEntityFactory.class.getName());

    static final QName ClassificationNodeList =
        new QName (NS, "ClassificationNodeList");
    static final QName ClassificationNode =
        new QName (NS, "ClassificationNode");
    static final QName ClassificationNodeChildList =
        new QName (NS, "ClassificationNodeChildList");

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
        
    void register (Disorder d) {
        getEntities (d);
        for (Disorder c : d.children)
            register (c);
    }

    @Override
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
