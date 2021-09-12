/*
           <Name lang="en">BTNT (broader term maps to a narrower term)</Name>
            <Name lang="en">E (exact mapping (the terms and the concepts are equivalent))</Name>
            <Name lang="en">ND (not yet decided/unable to decide)</Name>
            <Name lang="en">NTBT (narrower term maps to a broader term)</Name>
            <Name lang="en">NTBT/E (narrower term maps to a broader term because of an exact mapping with a synonym in the target terminology)</Name>
*/
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

public class OrphanetNomenclatureEntityFactory extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetNomenclatureEntityFactory.class.getName());

    static class Xref {
        public String rel;
        public String relICD10;        
        public String source;
        public String ref;
        public String status;
    }
    
    static class Nomenclature {
        public Term disorder = new Term ();
        public LinkedList<Xref> xrefs = new LinkedList<>();
        boolean isEmpty () { return xrefs.isEmpty(); }
    }

    public OrphanetNomenclatureEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetNomenclatureEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public OrphanetNomenclatureEntityFactory (File dir) throws IOException {
        super (dir);
    }

    void stitch (Xref xref, Entity... orpha) {
        String prefix = xref.source.toUpperCase();
        if ("ICD-10".equals(prefix)) {
            prefix = "ICD10CM";
        }
        
        Map<String, Object> attrs = new TreeMap<>();
        attrs.put(SOURCE, source.getKey());
        attrs.put(NAME, xref.rel);
        attrs.put("status", xref.status);
        if (xref.relICD10 != null && !"".equals(xref.relICD10))
            attrs.put("icd10", xref.relICD10);
        StitchKey key = R_closeMatch;
        if (xref.rel.startsWith("E"))
            key = R_exactMatch;
        String value = prefix+":"+xref.ref;
        for (Iterator<Entity> iter = find ("notation", value);
             iter.hasNext(); ) {
            Entity e = iter.next();
            for (Entity ent : orpha)
                // ent -> e
                ent.stitch(e, key, value, attrs);
        }
    }
    
    int stitch (Nomenclature nomen) throws Exception {
        Entity[] disorders = getEntities (nomen.disorder);
        for (Xref xref : nomen.xrefs) {
            stitch (xref, disorders);
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
        Nomenclature nomen = null;
        Xref xref = null;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();
                String tag = se.getName().getLocalPart();
                switch (tag) {
                case "Disorder":
                    nomen = new Nomenclature ();
                    break;
                case "ExternalReference":
                    xref = new Xref ();
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
                                .writeValueAsString(nomen));
                    if (!nomen.isEmpty()) {
                        int cnt = stitch (nomen);
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
                            nomen.disorder.orphaNumber = orpha;
                            break;
                        }
                    }
                    break;
                    
                case "Name":
                    switch (parent) {
                    case "Disorder":
                        nomen.disorder.name = value;
                        break;
                    case "DisorderMappingICDRelation":
                        xref.relICD10 = value;
                        break;
                    case "DisorderMappingRelation":
                        xref.rel = value;
                        break;
                    case "DisorderMappingValidationStatus":
                        xref.status = value;
                        break;
                    }
                    break;

                case "Source":
                    switch (parent) {
                    case "ExternalReference":
                        xref.source = value;
                        break;
                    }
                    break;

                case "Reference":
                    switch (parent) {
                    case "ExternalReference":
                        xref.ref = value;
                        break;
                    }
                    break;

                case "ExternalReference":
                    nomen.xrefs.push(xref);
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
                        +OrphanetNomenclatureEntityFactory.class.getName()
                        +" DBDIR XML");
            System.exit(1);
        }
        
        try (OrphanetNomenclatureEntityFactory orph =
             new OrphanetNomenclatureEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                DataSource ds = orph.register(file);
            }
        }
    }
}

