package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;

/**
 * annotate entities, e.g.,
 * sbt stitcher/"runMain ncats.stitcher.impl.AnnotationEntityFactory ncatskg-v6.db I_CODE data/mondo-rare.txt"
 */
public class AnnotationEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(AnnotationEntityFactory.class.getName());
    
    public AnnotationEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public AnnotationEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public AnnotationEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public DataSource register (StitchKey key, File file) throws IOException {
        DataSource ds= super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null) {
            try (InputStream is = new FileInputStream (file)) {
                LineTokenizer tokenizer = new LineTokenizer ();
                tokenizer.setInputStream(is);
                int cnt = 0;
                Set<String> missed = new LinkedHashSet<>();
                while (tokenizer.hasNext()) {
                    String[] toks = tokenizer.next();
                    if (toks.length > 0) {
                        int matches = 0;
                        for (Iterator<Entity> it = find (key, toks[0]);
                             it.hasNext(); ++matches) {
                            Entity e = it.next();
                            logger.info("+++++++ "+cnt+"/ "
                                        +toks[0]+" => "+e.getId());
                            e.addLabel(source.getName());
                        }
                        
                        if (matches > 0)
                            ++cnt;
                        else {
                            missed.add(toks[0]);
                        }
                    }
                }

                logger.info("### Data source "+ds.getName()
                            +" ("+ds.getKey()+") "
                            +"registered with "+cnt+"/"
                            +tokenizer.getLineCount()+" entities!");
                
                if (!missed.isEmpty()) {
                    logger.warning("*** "+missed.size()
                                   +" tokens are not mapped:");
                    for (String t : missed)
                        logger.warning(t);
                }

                if (cnt > 0) {
                    ds.set(INSTANCES, cnt);
                    updateMeta (ds);
                }
            }
        }
        else {
            logger.info("### Data source "+ds.getName()+" ("+ds.getKey()+") "
                        +"is already registered with "+count+" entities!");
        }
        return ds;        
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 3) {
            logger.info("Usage: "+AnnotationEntityFactory.class.getName()
                        +" DBDIR StitchKey AnnotationFile");
            System.exit(1);
        }
        
        try (AnnotationEntityFactory aef =
             new AnnotationEntityFactory (argv[0])) {
            aef.register(StitchKey.valueOf(argv[1]), new File (argv[2]));
        }
    }
}
