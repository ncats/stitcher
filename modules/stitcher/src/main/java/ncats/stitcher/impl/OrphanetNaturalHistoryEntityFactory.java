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

public class OrphanetNaturalHistoryEntityFactory extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetNaturalHistoryEntityFactory.class.getName());

    static class NaturalHistory {
        public Term disorder = new Term ();
        public LinkedList<Term> onsets = new LinkedList<>();
        public LinkedList<Term> deaths = new LinkedList<>();
        public LinkedList<Term> inheritances = new LinkedList<>();

        boolean isEmpty () {
            return onsets.isEmpty() && deaths.isEmpty() && inheritances.isEmpty();
        }
    }

    public OrphanetNaturalHistoryEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetNaturalHistoryEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetNaturalHistoryEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField (Props.URI);
        setNameField ("label");
        add (I_CODE, "notation");
    }
    
    void stitch (Entity d, Term t, String relname) throws Exception {
        boolean stitched = true;
        Entity[] entities = getEntities (t, "Orphanet");
        if (entities.length == 0) {
            // add new (transient) concept
            Map<String, Object> data = new TreeMap<>();
            data.put(Props.URI,
                     "http://www.orpha.net/ORDO/Orphanet_"+t.orphaNumber);
            data.put("notation", "Orphanet:"+t.orphaNumber);
            data.put("label", t.name);
            ncats.stitcher.Entity ent = register (data);
            //ent.addLabel(AuxNodeType.TRANSIENT);
            ent.addLabel("S_ORDO_ORPHANET"); // manually add this
            entities = new Entity[]{ent};
        }

        Map<String, Object> attrs = new TreeMap<>();
        attrs.put(NAME, relname);
        attrs.put(SOURCE, source.getKey());
        for (Entity e : entities)
            d.stitch(e, R_rel, "Orphanet:"+t.orphaNumber, attrs);
    }
    
    int stitch (NaturalHistory nh) throws Exception {
        Entity[] disorders = getEntities (nh.disorder);
        for (Entity d : disorders) {
            for (Term t : nh.onsets)
                stitch (d, t, "has_age_of_onset");
            for (Term t : nh.deaths)
                stitch (d, t, "has_age_of_death");
            for (Term t : nh.inheritances)
                stitch (d, t, "has_inheritance");
        }
        return disorders.length;
    }
    
    @Override
    protected int register (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        int count = 0;
        ObjectMapper mapper = new ObjectMapper ();       
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        NaturalHistory nh = null;
        Term term = null;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();
                String tag = se.getName().getLocalPart();
                switch (tag) {
                case "Disorder":
                    nh = new NaturalHistory ();
                    break;
                case "AverageAgeOfOnset":
                case "AverageAgeOfDeath":
                case "TypeOfInheritance":
                    term = new Term ();
                    break;
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                if (stack.isEmpty())
                    break; // we're done
                
                String value = buf.toString().trim();
                String tag = se.getName().getLocalPart();
                String parent = ((StartElement)stack.peek())
                    .getName().getLocalPart();

                //logger.info("----"+parent+"/"+tag+"/"+value);
                switch (tag) {
                case "Disorder":
                    logger.info(mapper.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(nh));
                    if (!nh.isEmpty()) {
                        int cnt = stitch (nh);
                        if (cnt > 0)
                            ++count;
                    }
                    break;
                    
                case "OrphaNumber":
                case "OrphaCode":
                    if (!"".equals(value)) {
                        Integer orpha = Integer.parseInt(value);
                        switch (parent) {
                        case "Disorder":
                            nh.disorder.orphaNumber = orpha;
                            break;
                        case "AverageAgeOfOnset":
                        case "AverageAgeOfDeath":
                        case "TypeOfInheritance":
                            term.orphaNumber = orpha;
                            break;
                        }
                    }
                    break;
                    
                case "Name":
                    switch (parent) {
                    case "Disorder":
                        nh.disorder.name = value;
                        break;
                    case "AverageAgeOfOnset":
                    case "AverageAgeOfDeath":
                    case "TypeOfInheritance":
                        term.name = value;
                        break;
                    }
                    break;

                case "AverageAgeOfOnset":
                    nh.onsets.push(term);
                    break;

                case "AverageAgeOfDeath":
                    nh.deaths.push(term);
                    break;

                case "TypeOfInheritance":
                    nh.inheritances.push(term);
                    break;
                } // endswitch()
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
                        +OrphanetNaturalHistoryEntityFactory.class.getName()
                        +" DBDIR XML");
            System.exit(1);
        }
        
        try (OrphanetNaturalHistoryEntityFactory orph =
             new OrphanetNaturalHistoryEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                DataSource ds = orph.register(file);
            }
        }
    }
}
