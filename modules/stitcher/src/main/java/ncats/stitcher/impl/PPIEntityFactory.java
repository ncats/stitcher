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

// GeneRIF interactions
// ftp://ftp.ncbi.nlm.nih.gov/gene/GeneRIF/interactions.gz
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
        setIdField ("id");
        add(I_GENE, "gene_id_2")
            .add(I_GENE, "interactant_id_7")
            //.add(I_PMID, "pubmed_id_list_14")
            .add(T_Keyword, "generif_text_16")
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
    
    protected int register (InputStream is) throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ();
        tokenizer.setInputStream(is);

        String[] header = null;
        int count = 0, lines = 0;
        Map<String, Object> data = new LinkedHashMap<>();
        for (; tokenizer.hasNext(); ++lines) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else {
                data.clear();
                for (int i = 0; i < header.length; ++i) {
                    Object value = "-".equals(toks[i]) ? null : toks[i];
                    if (header[i].equals("pubmed_id_list")) {
                        List<Long> pmids = new ArrayList<>();
                        for (String t : toks[i].split(",")) {
                            try {
                                pmids.add(Long.parseLong(t));
                            }
                            catch (NumberFormatException ex) {
                            }
                        }
                        value = !pmids.isEmpty() ?
                            pmids.toArray(new Long[0]) : null;
                    }
                    data.put(header[i]+"_"+(i+1), value);
                }
                data.put("source", source.getName());
                
                String src = (String) data.get("interaction_id_type_18");
                String id = (String) data.get("interaction_id_17");
                if (src != null && id != null) {
                    id = src.toUpperCase()+":"+id;
                    data.put("id", id);

                    // only consider human for now
                    if ("9606".equals(data.get("#tax_id_1"))
                        && "9606".equals(data.get("tax_id_6"))) {
                        Entity e = register (data);
                        logger.info("++++++++ "+count+"/"+lines+" "+id
                                    +" => "+e.getId()+" ++++++++");
                        ++count;
                    }
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
