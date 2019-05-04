package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * load PPI from mitab file format
 */
public class PPIEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(PPIEntityFactory.class.getName());

    public PPIEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public PPIEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public PPIEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("Interaction Identifiers");
        add(I_GENE, "_interactors_A")
            .add(I_GENE, "_interactors_B")
            .add(I_PMID, "_pmids")
            .add(I_CODE, "Interaction Identifiers")
            .add(T_Keyword, "Interaction Types")
            ;
    }
    
    List<Entity> getEntities (String value) {
        if (value.startsWith("entrez gene")) {
            int pos = value.indexOf(':');
            if (pos > 0) {
                value = "GENE"+value.substring(pos);
            }
        }
        else if (value.startsWith("uniprotkb:")) {
            value = value.toUpperCase();
        }

        List<Entity> entities = new ArrayList<>();        
        if (value != null) {
            Iterator<Entity> iter = find (I_GENE, value);
            if (!iter.hasNext()) {
                iter = find (N_Name, value);
            }
            
            while (iter.hasNext())
                entities.add(iter.next());
        }
        
        return entities;
    }

    static String[] parseGenes (String text) {
        List<String> genes = new ArrayList<>();
        for (String s : text.split("\\|")) {
            if (s.startsWith("entrez gene")) {
                int pos = s.indexOf(':');
                if (pos > 0) {
                    int end = s.indexOf('(', pos);
                    String gene = end < 0
                        ? s.substring(pos+1) : s.substring(pos+1, end);
                    try {
                        int id = Integer.parseInt(gene);
                        genes.add("GENE:"+id);
                    }
                    catch (NumberFormatException ex) {
                        genes.add(gene);
                    }
                }
            }
        }
        return genes.toArray(new String[0]);
    }

    protected void parse (Map<String, Object> data,
                          String[] header, String[] toks) {
        data.clear();
        for (int i = 0; i < header.length; ++i) {
            Object value = "-".equals(toks[i]) ? null : toks[i];
            if (value != null) {
                switch (header[i]) {
                case "Publication Identifiers":
                    { List<Long> pmids = new ArrayList<>();
                        for (String t : toks[i].split("\\|")) {
                            if (t.startsWith("pubmed:")) {
                                try {
                                    long id = Long.parseLong(t.substring(7));
                                    pmids.add(id);
                                }
                                catch (NumberFormatException ex) {
                                }
                            }
                        }
                        
                        if (!pmids.isEmpty()) {
                            data.put("_pmids",
                                     pmids.toArray(new Long[0]));
                        }
                    }
                    break;
                    
                case "ID Interactor A":
                    { String[] genes = parseGenes (toks[i]);
                        if (genes.length > 0)
                            data.put("_interactors_A", genes);
                    }
                    break;
                    
                case "ID Interactor B":
                    { String[] genes = parseGenes (toks[i]);
                        if (genes.length > 0)
                            data.put("_interactors_B", genes);
                    }
                    break;
                    
                case "Alt IDs Interactor A":
                case "Aliases Interactor A":
                    { String[] genes = parseGenes (toks[i]);
                        if (genes.length > 0) {
                            data.put
                                ("_interactors_A",
                                 Util.merge(data.get("_interactors_A"), genes));
                        }
                    }
                    break;
                    
                case "Alt IDs Interactor B":
                case "Aliases Interactor B":
                    { String[] genes = parseGenes (toks[i]);
                        if (genes.length > 0) {
                            data.put
                                ("_interactors_B",
                                 Util.merge(data.get("_interactors_B"), genes));
                        }
                    }
                    break;
                }
            }
            data.put(header[i], value);
        }
        data.put("_source", source.getName());
    }
    
    protected int register (InputStream is) throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ();
        tokenizer.setInputStream(is);

        String[] header = null;
        int count = 0, lines = 0;
        Map<String, Object> data = new LinkedHashMap<>();
        for (; tokenizer.hasNext(); ++lines) {
            String[] toks = tokenizer.next();
            if (header == null) {
                if (toks[0].charAt(0) == '#')
                    toks[0] = toks[0].substring(1);
                header = toks;
            }
            else {
                parse (data, header, toks);
                String taxa = (String) data.get("Taxid Interactor A");
                String taxb = (String) data.get("Taxid Interactor B");
                if ("taxid:9606".equals(taxa) && taxa.equals(taxb)) {
                    Entity e = register (data);
                    logger.info("++++++ "
                                +data.get("Interaction Identifiers")+" "
                                +count+"/"+lines+" => "+e.getId());
                    ++count;
                }
                ++lines;
            }
        }
        return count;
    }

    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds= super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null || count == 0) {
            count = register (ds.openStream());
            ds.set(INSTANCES, count);
            updateMeta (ds);
        }
        else {
            logger.info("### Data source "+ds.getName()+" ("+ds.getKey()+") "
                        +"is already registered with "+count+" entities!");
        }
        return ds;        
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+PPIEntityFactory.class.getName()
                        +" DBDIR FILE");
            System.exit(1);
        }
        
        try (PPIEntityFactory pef = new PPIEntityFactory (argv[0])) {
            pef.register(new File (argv[1]));
        }
    }
}
