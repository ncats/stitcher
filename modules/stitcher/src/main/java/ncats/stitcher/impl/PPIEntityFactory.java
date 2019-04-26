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
        Map<String, Object> attr = new HashMap<>();
        for (; tokenizer.hasNext(); ++lines) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else {
                attr.clear();
                for (int i = 0; i < header.length; ++i)
                    attr.put(header[i], toks[i]);
                attr.put("source", source.getName());

                List<Entity> intacta = getEntities (toks[0]);
                List<Entity> intactb = getEntities (toks[1]);
                for (Entity a : intacta) {
                    for (Entity b : intactb) {
                        // toks[13] interaction id
                        if (!a.equals(b))
                            a.stitch(b, R_ppi, toks[13], attr);
                    }
                }

                if (!intacta.isEmpty() && !intactb.isEmpty()) {
                    logger.info("++++++++ "+count+"/"+lines+" "+toks[0]
                                +" ("+intacta.size()
                                +") <-> "+toks[1]+" ("+intactb.size()+")");
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
                        +" DBDIR MINTAB_FILES...");
            System.exit(1);
        }
        
        try (PPIEntityFactory pef = new PPIEntityFactory (argv[0])) {
            pef.register(new File (argv[1]));
        }
    }
}
