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
            String line = tokenizer.getCurrentLine();
            String[] toks = tokenizer.next();
            if (line.startsWith("#description:")
                || line.startsWith("#date:")
                || line.startsWith("#tracker:")
                || line.startsWith("#HPO-version:")) {
                int pos = line.indexOf(':');
                source.set(line.substring(1, pos),
                           line.substring(pos+1).trim());
            }
            else {
                /*
                 * 0 #DatabaseID     
                 * 1 DiseaseName     
                 * 2 Qualifier
                 * 3 HPO_ID
                 * 4 Reference
                 * 5 Evidence
                 * 6 Onset
                 * 7 Frequency
                 * 8 Sex
                 * 9 Modifier
                 * 10 Aspect
                 * 11 Biocuration
                 */
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
                    List<Entity> phenotypes = getEntities (I_CODE, toks[3]);
                    logger.info(toks[0]+"="+diseases.size()+" "
                                +toks[3]+"="+phenotypes.size());
                    attr.clear();
                    for (int i = 0; i < header.length; ++i) {
                        if (i == 0)
                            // skip # char
                            attr.put(header[i].substring(1), toks[i]);
                        else
                            attr.put(header[i], toks[i]);
                    }
                    attr.put(SOURCE, source.getKey());
                    for (Entity p : phenotypes) {
                        for (Entity d : diseases) {
                            if (!p.equals(d)) {
                                d.stitch(p, R_hasPhenotype, toks[3], attr);
                                d.addLabel(source.getLabel());
                            }
                        }
                        p.addLabel(source.getLabel());
                    }
                    ++count;
                }
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
                        +" DBDIR phenotype.hpoa");
            System.exit(1);
        }
        
        try (HPOEntityFactory hef = new HPOEntityFactory (argv[0])) {
            hef.register(new File (argv[1]));
        }
    }
}
