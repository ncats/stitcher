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

public class OrphanetDiseaseGeneAssociationEntityFactory
    extends OrphanetEntityFactory {
    static final Logger logger =
        Logger.getLogger(OrphanetDiseaseGeneAssociationEntityFactory.class.getName());

    static final QName Id = new QName (NS, "id");
    static final QName DiseaseGeneAssociationList = new QName (NS, "DiseaseGeneAssociationList");
    static final QName DiseaseGeneAssociation = new QName (NS, "DiseaseGeneAssociation");
    static final QName SourceOfValidation = new QName (NS, "SourceOfValidation");
    static final QName Gene = new QName (NS, "Gene");
    static final QName Symbol = new QName (NS, "Symbol");
    static final QName DiseaseGeneAssociationType =
        new QName (NS, "DiseaseGeneAssociationType");
    static final QName DiseaseGeneAssociationStatus =
        new QName (NS, "DiseaseGeneAssociationStatus");

    static class DiseaseGeneAssociation {
        public Set<Long> pmids = new TreeSet<>();
        public String symbol;
        public String type;
        public String status;

        Map<String, Object> toMap () {
            Map<String, Object> data = new LinkedHashMap<>();
            if (!pmids.isEmpty())
                data.put("SourceOfValidation", pmids.stream().map(p -> "PMID:"+p)
                         .toArray(String[]::new));
            if (type != null)
                data.put("DiseaseGeneAssociationType", type);
            if (status != null)
                data.put("DiseaseGeneAssociationValidationStatus", status);
            return data;
        }
    }
    
    static class DiseaseGeneAssociationDisorder extends Disorder {
        public LinkedList<DiseaseGeneAssociation> associations = new LinkedList<>();
    }
    
    public OrphanetDiseaseGeneAssociationEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }
    
    public OrphanetDiseaseGeneAssociationEntityFactory (String dir)
        throws IOException {
        super (dir);
    }
    
    public OrphanetDiseaseGeneAssociationEntityFactory (File dir)
        throws IOException {
        super (dir);
    }

    Entity[] getGenes (String symbol) {
        List<Entity> genes = new ArrayList<>();
        for (Iterator<Entity> iter = find ("label", symbol);
             iter.hasNext(); ) {
            Entity e = iter.next();
            if (e.is("S_OGG")) { // for now we only get the gene from S_OGG source
                genes.add(e);
            }
        }
        return genes.toArray(new Entity[0]);
    }

    @Override
    protected int register (InputStream is) throws Exception {
        XMLEventReader events =
            XMLInputFactory.newInstance().createXMLEventReader(is);
        Map<String, Object> props = new HashMap<>();

        Pattern re = Pattern.compile("([\\d]+)\\[([^\\]]+)");
        int count = 0;
        ObjectMapper mapper = new ObjectMapper ();
        LinkedList<XMLEvent> stack = new LinkedList<>();        
        StringBuilder buf = new StringBuilder ();
        LinkedList<DiseaseGeneAssociationDisorder> dstack = new LinkedList<>();

        for (XMLEvent ev; events.hasNext(); ) {
            ev = events.nextEvent();
            if (ev.isStartElement()) {
                stack.push(ev);
                buf.setLength(0);
                
                StartElement se = ev.asStartElement();                
                QName qn = se.getName();
                if (Disorder.equals(qn)) {
                    DiseaseGeneAssociationDisorder d =
                        new DiseaseGeneAssociationDisorder ();
                    dstack.push(d);
                }
                else if (DiseaseGeneAssociationList.equals(qn)) {
                }
                else if (DiseaseGeneAssociation.equals(qn)) {
                    DiseaseGeneAssociation ass = new DiseaseGeneAssociation ();
                    dstack.peek().associations.push(ass);
                }
            }
            else if (ev.isEndElement()) {
                StartElement se = (StartElement) stack.pop();
                StartElement parent = (StartElement) stack.peek();
                String value = buf.toString();

                DiseaseGeneAssociationDisorder pd = dstack.peek();
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
                        for (DiseaseGeneAssociation ass : pd.associations) {
                            Entity[] genes = getGenes (ass.symbol);
                            if (genes.length == 0) {
                                logger.warning("** Gene "+ass.symbol
                                               +" has not matching entities!");
                            }
                            else {
                                props.clear();
                                props.putAll(ass.toMap());
                                props.put(SOURCE, source.getKey());
                                for (Entity g : genes) {
                                    for (Entity e : ents) {
                                        g.stitch(e, R_rel,
                                                 "gene_associated_with_disease",
                                                 props);
                                    }
                                }
                            }
                        }
                        ++count;
                        logger.info
                            ("+++++ "+String.format("%1$5d: ", count)
                             +pd.orphaNumber+": "+pd.name+"..."
                             +pd.associations.size()+" disease-gene association(s)");
                    }
                }
                else if (OrphaNumber.equals(qn)) {
                    if (Disorder.equals(pn))
                        pd.orphaNumber = Integer.parseInt(value);
                }
                else if (Name.equals(qn)) {
                    if (Disorder.equals(pn)) {
                        pd.name = value;
                    }
                    else if (DisorderType.equals(pn)) {
                        pd.type = value;
                    }
                    else if (DiseaseGeneAssociationType.equals(pn)) {
                        pd.associations.peek().type = value;
                    }
                    else if (DiseaseGeneAssociationStatus.equals(pn)) {
                        pd.associations.peek().status = value;
                    }
                }
                else if (SourceOfValidation.equals(qn)) {
                    for (String tok : value.split("_")) {
                        Matcher m = re.matcher(tok);
                        while (m.find()) {
                            String source = m.group(2);
                            if ("PMID".equals(source)) {
                                String pmid = m.group(1);
                                try {
                                    pd.associations.peek().pmids.add
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
                        +OrphanetDiseaseGeneAssociationEntityFactory.class.getName()
                        +" DBDIR XML");
            System.exit(1);
        }
        
        try (OrphanetDiseaseGeneAssociationEntityFactory orph =
             new OrphanetDiseaseGeneAssociationEntityFactory (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                DataSource ds = orph.register(file);
            }
        }
    }
}
