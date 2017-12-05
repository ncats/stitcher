package ncats.stitcher;

import java.io.Serializable;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.net.URI;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

import com.sleepycat.je.*;
import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.serial.ClassCatalog;
import com.sleepycat.bind.serial.SerialBinding;
import com.sleepycat.bind.serial.StoredClassCatalog;
import com.sleepycat.collections.StoredEntrySet;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.collections.StoredSortedKeySet;

public class UntangleCompoundStitches extends UntangleCompoundAbstract {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundStitches.class.getName());

    final protected Double threshold;
    final protected Map<Object, Integer> counts = new HashMap<>();
    final protected Entity seed;

    static class TripleEntry implements Serializable,
                                        Comparable<TripleEntry> {
        public final Entity.Triple triple;
        public final double score;
        
        public TripleEntry () {
            this (null, -1.0);
        }
        
        public TripleEntry (Entity.Triple triple, double score) {
            this.triple = triple;
            this.score = score;
        }

        public int compareTo (TripleEntry t) {
            if (t.score > score) return 1;
            else if (t.score < score) return -1;
            return triple.compareTo(t.triple);
        }

        public String toString () {
            return "("+triple.source()+","+triple.target()+","+score+")";
        }
    }
    
    protected double total;
    protected Environment env;
    protected StoredClassCatalog catalog;
    protected SerialBinding entryBinding;
    protected StoredSortedKeySet<TripleEntry> triples;
    protected Database tripleDb;

    public UntangleCompoundStitches (DataSource dsource) {
        this (dsource, null, null);
    }

    public UntangleCompoundStitches (DataSource dsource, Entity seed) {
        this (dsource, seed, null);
    }
    
    public UntangleCompoundStitches (DataSource dsource, Double threshold) {
        this (dsource, null, threshold);
    }
    
    public UntangleCompoundStitches (DataSource dsource,
                                     Entity seed, Double threshold) {
        super (dsource);
        this.seed = seed;
        this.threshold = threshold;
        try {
            initDbEnv ();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't initialize database!", ex);
        }
    }

    void initDbEnv () throws Exception {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setTransactional(true);
        envConfig.setAllowCreate(true);
        envConfig.setCacheSize(8*1024*1024);
        env = new Environment
            (Files.createTempDirectory("stitcher").toFile(), envConfig);
                
        Transaction tx = env.beginTransaction(null, null);
        try {
            DatabaseConfig dbconf = new DatabaseConfig ();
            dbconf.setTransactional(true);
            dbconf.setAllowCreate(true);
            Database db = env.openDatabase(tx, "ClassCataglog", dbconf);
            catalog = new StoredClassCatalog (db);

            dbconf = (DatabaseConfig)dbconf.clone();
            dbconf.setSortedDuplicates(true);
            tripleDb = env.openDatabase(tx, "StoredSortedTripleEntry", dbconf);
            entryBinding = new SerialBinding (catalog, TripleEntry.class);
            triples = new StoredSortedKeySet (tripleDb, entryBinding, true);
        }
        finally {
            tx.commit();
        }
    }


    double calcScore (Entity.Triple triple) {
        Map<StitchKey, Object> values = triple.values();        
        double score = 0.;

        Entity target = ef.entity(triple.target());
        Entity source = ef.entity(triple.source());
        for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
            StitchKey key = me.getKey();
            Object val = me.getValue();
            double s, t = 0.;
            if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i)
                    t += Math.log(counts.get(Array.get(val, i))/total);
                s = (double)len/(Util.getLength(source.get(key))
                                 + Util.getLength(target.get(key)) - len);
            }
            else {
                t = Math.log(counts.get(val)/total);
                s = 1.0/(Util.getLength(source.get(key))
                         + Util.getLength(target.get(key)) - 1.0);
            }
            score += s*me.getKey().priority*t;
        }
        return -score;
    }

    protected void traverse (EntityVisitor visitor, StitchKey... keys) {
        if (seed != null) {
            seed.traverse(visitor, keys);
        }
        else {
            ef.traverse(visitor, keys);
        }
    }

    protected void updateCounts (EntityFactory ef) {
        counts.clear();

        logger.info("############## STITCH COUNTS ##############");
        /*
        traverse ((traversal, triple) -> {
                Map<StitchKey, Object> values = triple.values();
                for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
                    Object val = me.getValue();
                    if (val.getClass().isArray()) {
                        int len = Array.getLength(val);
                        for (int i = 0; i < len; ++i) {
                            Integer c = counts.get(Array.get(val, i));
                            counts.put(Array.get(val, i), c==null ? 1:c+1);
                        }
                    }
                    else {
                        Integer c = counts.get(val);
                        counts.put(val, c==null ? 1:c+1);
                    }
                }
                return true;
            });
        */
        
        counts.putAll(seed != null ? seed.getComponentStitchCounts()
                      : ef.getStitchCounts());
        //logger.info("$$$$ COUNTS ==> "+counts);       

        total = 0.;
        for (Integer v : counts.values())
            total += v;

        logger.info("$$$$ total stitch count ==> "+total);
    }

    @Override
    public void untangle (EntityFactory ef, BiConsumer<Long, long[]> consumer) {
        this.ef = ef;
        uf.clear();

        updateCounts (ef);

        Set<Entity> roots = new HashSet<>();
        Set<Entity> unsure = new HashSet<>();
        traverse ((traversal, triple) -> {
                Entity source = ef.entity(triple.source(T_ActiveMoiety));
                Entity target = ef.entity(triple.target(T_ActiveMoiety));
                
                Entity[] out = source.outNeighbors(T_ActiveMoiety);
                Entity[] in = target.inNeighbors(T_ActiveMoiety);

                boolean root = isRoot (target);
                logger.info(" ("+out.length+") "+source.getId()
                            +" -> "+target.getId()+" ["
                            +root+"] ("+in.length+")");
                if (root)
                    roots.add(target);
                else if (isRoot (source)) {
                    logger.warning
                        ("Entity "+source.getId()+" likely to "
                         +"have "+out.length
                         +" flipped active moiety relationships");
                    for (Entity e : out) {
                        uf.union(source.getId(), e.getId());
                    }
                    roots.add(source);
                }

                if (out.length == 1) {
                    // first collapse single/terminal active moieties
                    uf.union(target.getId(), source.getId());
                }
                else if (out.length > 1) {
                    unsure.add(source);                     
                }
                
                return true;
            }, T_ActiveMoiety);
        dump ("##### active moiety stitching");

        logger.info("########### STITCHING TRIPLES #############");
        traverse ((traversal, triple) -> {
                double score = calcScore (triple);
                Entity source = ef.entity(triple.source());
                Entity target = ef.entity(triple.target());
                
                boolean a = roots.contains(source);
                if (!a && (a = isRoot (source))) {
                    uf.add(source.getId(), 2);
                    roots.add(source);
                    logger.info("Entity "+source.getId()+" is root!");
                }

                boolean b = roots.contains(target);
                if (!b && (b = isRoot (target))) {
                    uf.add(target.getId(), 2);
                    roots.add(target);
                    logger.info("Entity "+target.getId()+" is root!");
                }

                logger.info("..."+source.getId()+" "+target.getId()
                            +" score="+score+" "
                            +Util.toString(triple.values()));
                if (/*score > threshold &&*/ !(a && b)) {
                    triples.add(new TripleEntry (triple, score));
                }
                /*
                else {
                    if (!a) uf.add(source.getId());
                    if (!b) uf.add(target.getId());
                }
                */
                
                return true;
            });

        double thres = 0.;
        if (!triples.isEmpty()) {
            double min = triples.first().score;
            double max = triples.last().score;

            double mean = 0.;
            for (TripleEntry te : triples)
                mean += te.score;
            mean /= triples.size();
            
            logger.info("$$$$$$ SCORE: MIN = "+min
                        +" MAX = "+max
                        +" MEAN = "+mean+" $$$$$$$$");
            if (min < 1.)
                thres = Math.max(5.0*min, mean - 5.0*min); // eh..??
            else
                thres = min;
        }

        if (threshold != null)
            thres = threshold;

        logger.info("$$$$$$ THRESHOLD = "+thres+" ");

        Transaction tx = env.beginTransaction(null, null);
        try {
            Cursor cursor = tripleDb.openCursor
                (tx, CursorConfig.READ_UNCOMMITTED);
            DatabaseEntry key = new DatabaseEntry ();
            DatabaseEntry data = new DatabaseEntry();
            if (OperationStatus.SUCCESS == cursor.getLast(key, data, null)) {
                do {
                    TripleEntry entry =
                        (TripleEntry) entryBinding.entryToObject(key);
                    Entity.Triple triple = entry.triple;
                    //logger.info("----- "+entry+" -----");
                    
                    Entity source = ef.entity(triple.source());
                    Entity target = ef.entity(triple.target());
                    
                    double score = entry.score;
                    if (score > thres
                        && triple.values().containsKey(H_LyChI_L3)
                        && !triple.values().containsKey(H_LyChI_L4)) {
                        // adjust the score to require strong evidence if there
                        // might be a chance of structural problems
                        score /= 3.0; // eh..??
                        logger.warning("** score is adjusted from "
                                       +entry.score+" to "+score);
                    }
                    
                    if (score > thres && union (target, source)) {
                        // now let's interogate this a big more 
                        logger.info("## merging "+source.getId()
                                    +" "+target.getId()+".. "+score);
                    }
                    else {
                        uf.add(source.getId());
                        uf.add(target.getId());
                    }
                }
                while (OperationStatus.SUCCESS
                       == cursor.getPrev(key, data, null));
            }
            cursor.close();
        }
        finally {
            tx.commit();
        }

        // now resolve entities that point to multiple active moieties
        for (Entity e : unsure) {
            Entity[] nb = e.outNeighbors(T_ActiveMoiety);
            // just assign to arbitrary or based on rank?
            uf.union(e.getId(), nb[0].getId());
        }

        createStitches (consumer);
    }

    public void shutdown () {
        try {
            catalog.close();
            tripleDb.close();
            env.close();
            env.getHome().delete();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Closing databases", ex);
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundStitches.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            int version = Integer.parseInt(argv[1]);
            DataSource dsource =
                ef.getDataSourceFactory().register("stitch_v"+version);

            if (argv.length > 2) {
                for (int i = 2; i < argv.length; ++i) {
                    long id = Long.parseLong(argv[i]);
                    Entity e = ef.entity(id);
                    logger.info("################ COMPONENT "
                                +id+" ("+e.get(RANK)+") ################");
                    UntangleCompoundStitches ucs =
                        new UntangleCompoundStitches (dsource, e);
                    ef.untangle(ucs);
                    ucs.shutdown();
                }
            }
            else {
                List<Long> comps = new ArrayList<>();
                ef.components(comps);
                logger.info("### there are "+comps.size()+" components ###");
                for (int i = 0; i < comps.size(); ++i) {
                    long id = comps.get(i);
                    Entity e = ef.entity(id);
                    logger.info("################ COMPONENT "
                                +String.format("%1$5d", i+1)+": "+
                                +id+" ("+e.get(RANK)+") ################");
                    UntangleCompoundStitches ucs =
                        new UntangleCompoundStitches (dsource, e);
                    ef.untangle(ucs);
                    ucs.shutdown();
                }
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
