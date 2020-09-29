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

public class HMDBEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(HMDBEntityFactory.class.getName());

    public HMDBEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public HMDBEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public HMDBEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setNameField ("name");
        setIdField ("id");
        setStrucField ("smiles");
        add (N_Name, "name")
            .add(N_Name, "synonym")
            .add(I_CAS, "cas_registry_number")
            .add(I_GENE, "gene_name")
            .add(I_CODE, "accession")
            .add(H_InChIKey, "inchikey")
            .add(T_Keyword, "patient_information")
            .add(T_Keyword, "biospeciment")
            .add(T_Keyword, "tissue")
            .add(T_Keyword, "cellular")
            ;
    }
    
    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        try {
            XMLEventReader events = XMLInputFactory.newInstance().
                createXMLEventReader(ds.openStream());
            int count = register (events);
            logger.info(count+" entities registered!");
            ds.set(INSTANCES, count);
            updateMeta (ds);
            logger.info("$$$ "+count+" entities registered for "+ds);
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't process file "+file, ex);
        }
        return ds;
    }

    protected int register (XMLEventReader events) throws Exception {
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        Map<String, Object> data = new TreeMap<>();
        int count = 0, nreg = 0;
        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                StartElement parent = (StartElement) stack.peek();
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();
                switch (se.getName().getLocalPart()) {
                case "metabolite":
                    ++count;
                    data.clear();
                    break;
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString().trim();

                String tag = se.getName().getLocalPart();
                switch (tag) {
                case "accession":
                    if ("metabolite".equals(parent.getName().getLocalPart())) {
                        data.put("id", value);
                    }
                    else {
                        Object old = data.get(tag);
                        data.put(tag, old != null ? Util.merge(old, value) : value);
                    }
                    break;
                    
                case "name":
                case "description":
                case "alternative_parent":
                case "substituent":
                case "external_descriptor":
                case "chemical_formula":
                case "iupac_name":
                case "cas_registry_number":
                case "smiles":
                case "inchikey":
                case "cellular":
                case "biospecimen":
                case "tissue":
                case "gene_name":
                    { Object old = data.get(tag);
                        data.put(tag, old != null ? Util.merge(old, value) : value);
                    }
                break;
                
                case "synonym":
                    if ("metabolite".equals(stack.get(1).asStartElement()
                                            .getName().getLocalPart())) {
                        Object old = data.get(tag);
                        data.put(tag, old != null ? Util.merge(old, value) : value);
                    }
                    break;
                    
                case "average_molecular_weight":
                case "monisotopic_molecular_weight":
                    data.put(tag, Double.parseDouble(value));
                break;
                
                case "patient_information":
                    for (XMLEvent xe : stack) {
                        if ("abnormal_concentrations"
                            .equals(xe.asStartElement().getName().getLocalPart())) {
                            Object old = data.get(tag);
                            data.put(tag, old != null ? Util.merge(old, value) : value);
                        }
                    }
                    break;

                case "metabolite":
                    //logger.info("**** " + data);
                    { ncats.stitcher.Entity e = register (data);
                        if (e != null) {
                            ++nreg;
                            logger.info("+++++++ "+nreg+"/"+count+" "+e.getId()+" "
                                        +data.get("id") +" +++++++");
                        }
                    }
                    break;
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
            logger.info("Usage: "+HMDBEntityFactory.class.getName()
                        +" DBDIR HMDB.xml");
            System.exit(1);
        }
        
        try (HMDBEntityFactory hmdb = new HMDBEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                if (argv[i].startsWith("cache=")) {
                    hmdb.setCache(argv[i].substring(6));
                    if ((i+1) >= argv.length) {
                        logger.warning("No input file specified!");
                        System.exit(1);
                    }
                }
                else {
                    File file = new File (argv[i]);
                    logger.info("Registering "+file+"...");
                    hmdb.register(file);
                }
            }
        }
    }
}
