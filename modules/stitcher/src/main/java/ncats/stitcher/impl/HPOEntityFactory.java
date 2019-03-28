package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * this is for HPO annotations; please use OntEntityFactory for loading 
 * ontology entities via hpo.owl
 */
public class HPOEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(HPOEntityFactory.class.getName());
    
    public HPOEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public HPOEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public HPOEntityFactory (File dir) throws IOException {
        super (dir);
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
            /*
            else if (toks.length != header.length) {
                logger.warning(lines+": expecting "+header.length
                               +" columns but instead found "+toks.length+"!");
            }
            */
            else {
                // diseaseId gene-symbol gene-id(entrez)HPO-ID HPO-term-name
                List<Entity> diseases = getEntities (I_CODE, toks[0]);
                //List<Entity> genes = getEntities (I_GENE, toks[1]);
                List<Entity> phenotypes = getEntities (I_CODE, toks[3]);
                logger.info(toks[0]+"="+diseases.size()+" "
                            +toks[3]+"="+phenotypes.size());
                attr.clear();
                for (int i = 0; i < header.length; ++i)
                    attr.put(header[i], toks[i]);
                for (Entity p : phenotypes) {
                    for (Entity d : diseases) {
                        if (!p.equals(d)) {
                            d.stitch(p, R_rel, "has_phenotype", attr);
                            d.addLabel(source.getName());
                        }
                    }
                    p.addLabel(source.getName());
                }
                ++count;
            }
        }
        return count;
    }

    List<Entity> getEntities (StitchKey key, Object value) {
        Iterator<Entity> iter = find (key.name(), value);
        List<Entity> entities = new ArrayList<>();
        while (iter.hasNext())
            entities.add(iter.next());
        return entities;
    }
    
    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds= super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null) {
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
            logger.info("Usage: "+HPOEntityFactory.class.getName()
                        +" DBDIR HPO-Annotations");
            System.exit(1);
        }
        
        try (HPOEntityFactory hef = new HPOEntityFactory (argv[0])) {
            hef.register(new File (argv[1]));
        }
    }
}
