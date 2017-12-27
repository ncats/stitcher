package ncats.stitcher;

import java.io.Serializable;
import java.io.File;
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
            double s = 0., t = 0.;
            if (val.getClass().isArray()) {
                int len = Array.getLength(val), l = 0;
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (v != null) {
                        Integer c = counts.get(v);
                        if (c != null) {
                            t += Math.log(c/total);
                            ++l;
                        }
                        else {
                            logger.log(Level.SEVERE, 
                                       "** stitch value \""+v+"\" has zero count!");
                        }
                    }
                }
                s = (double)l/(Util.getLength(source.get(key))
                               + Util.getLength(target.get(key)) - l);
            }
            else {
                Integer c = counts.get(val);
                if (c != null) {
                    t = Math.log(c/total);
                    s = 1.0/(Util.getLength(source.get(key))
                             + Util.getLength(target.get(key)) - 1.0);
                }
                else {
                    logger.log(Level.SEVERE, 
                               "*** stitch value \""+val+"\" has zero count!");
                }
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
        StitchValueVisitor visitor = (key, value, count) -> {
            Integer c = counts.get(value);
            counts.put(value, c == null ? count : (c+count));
        };

        if (seed != null)
            seed.componentStitchValues(visitor);
        else 
            ef.stitchValues(visitor);
        //logger.info("$$$$ COUNTS ==> "+counts);       

        total = 0.;
        for (Integer v : counts.values())
            total += v;

        logger.info("$$$$ total stitch count ==> "+total);
    }

    static void lychiSuffixes (Map<String, Integer> suffixes, Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(value); ++i) {
                    String v = Array.get(value, i).toString();
                    String s = v.substring(v.length() - 2);
                    Integer c = suffixes.get(s);
                    suffixes.put(s, c==null ?1:c+1);
                }
            }
            else {
                String v = value.toString();
                String s = v.substring(v.length() - 2);
                Integer c = suffixes.get(s);
                suffixes.put(s, c==null ? 1:c+1);
            }
        }
    }

    static Map<String, Integer> lychiSuffixes (Object value) {
        Map<String, Integer> suffixes = new TreeMap<>();
        lychiSuffixes (suffixes, value);
        return suffixes;
    }

    static int countGroup1Metal (Entity ent) {
        Object moieties = ent.get(MOIETIES);

        int count = 0;
        if (moieties != null) {
            if (moieties.getClass().isArray()) {
                int len = Array.getLength(moieties);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(moieties, i);
                    try {
                        boolean g1 = Util.isGroup1Metal
                            (Util.getMol(v.toString()));
                        if (g1)
                            ++count;
                    }
                    catch (Exception ex) {
                        logger.log(Level.SEVERE, "Bogus moiety "+v, ex);
                    }
                }
            }
            else {
                try {
                    if (Util.isGroup1Metal
                        (Util.getMol(moieties.toString())))
                        ++count;
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Can't get process moieties: "
                               +moieties, ex);
                }
            }
        }

        return count;
    }

    boolean checkStructureCompatibility 
        (Object l4, Entity source, Entity target) {
        Object delta = Util.delta(source.get(H_LyChI_L4), l4);
        
        Map<String, Integer> suffixes = new TreeMap<>();
        if (delta != null && delta != Util.NO_CHANGE) {
            lychiSuffixes (suffixes, delta);
        }
        
        delta = Util.delta(target.get(H_LyChI_L4), l4);
        if (delta != null && delta != Util.NO_CHANGE)
            lychiSuffixes (suffixes, delta);
        
        boolean compatible = !suffixes.containsKey("-N")
            && !suffixes.containsKey("-M");
        logger.info("~~~~~~~ "+source.getId()+" "
                    +target.getId() +" l4 suffixes="+Util.toString(suffixes)
                    +" source="+Util.toString(source.get(H_LyChI_L4))
                    +" target="+Util.toString(target.get(H_LyChI_L4))
                    +" compatible="+compatible);

        return compatible;
    }

    boolean transitiveStructureCompatibility 
        (Object l4, Entity source, Entity target) {
        for (Entity nb : source.neighbors()) {
            if (!nb.equals(target) && uf.root(nb.getId()) != null) {
                Map<StitchKey, Object> values = target.keys(nb);
                if (values.containsKey(H_LyChI_L4)) {
                    if (!checkStructureCompatibility 
                        (values.get(H_LyChI_L4), nb, target)) {
                        logger.info("!!!!!!! "+source.getId()+" " 
                                    +target.getId()+" not l4 compatible "
                                    +"due to "+nb.getId()+" !!!!!!!");
                        return false;
                    }
                }
                else if (values.containsKey(H_LyChI_L3)) {
                    
                }
            }
        }
        return true;
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
        DoubleAdder mean = new DoubleAdder ();
        AtomicInteger count = new AtomicInteger ();
        traverse ((traversal, triple) -> {
                double score = calcScore (triple);
                Entity source = ef.entity(triple.source());
                Entity target = ef.entity(triple.target());

                logger.info("..."+source.getId()+" "+target.getId()
                            +" score="+score+" "
                            +Util.toString(triple.values()));
                double m = mean.sumThenReset();
                if (count.getAndIncrement() > 0) {
                    mean.add(((count.get()-1)*m + score)/count.get());
                }
                else {
                    mean.add(score);
                }
                
                if (unsure.contains(source)) {
                    logger.info("Defer entity "+source.getId()
                                +" because it has multiple active moieties!");
                }
                else if (unsure.contains(target)) {
                    logger.info("Defer entity "+target.getId()
                                +" because it has multiple active moieties!");
                }
                else {
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
                    
                    if (!a && !b) {
                        triples.add(new TripleEntry (triple, score));
                    }
                }
                
                return true;
            });

        double thres = 0.;
        if (count.get() > 0) {
            double min = triples.first().score;
            double max = triples.last().score;

            double med = 0.0;
            int mid = triples.size() / 2, i = 0;
            TripleEntry p = null;
            for (TripleEntry te : triples) {
                if (++i < mid) {
                }
                else {
                    med = triples.size() % 2 == 0
                        ? te.score + (p.score - te.score)/2.0
                        : p.score;
                    break;
                }
                p = te;
            }

            logger.info("============= SCORE: MIN = "+min
                        +" MAX = "+max
                        +" MEAN = "+mean+" MED = "+med+" =============");

            /*
            if (min < 1.)
                thres = Math.max
                    (5.0*min, mean.doubleValue() - 5.0*min); // eh..??
            else
                thres = min;
            */
            thres = Math.max(med - min, mean.doubleValue() - med);
        }

        if (threshold != null)
            thres = threshold;

        logger.info("################ THRESHOLD = "+thres
                    +" ##################");

        Transaction tx = env.beginTransaction(null, null);
        try {
            Cursor cursor = tripleDb.openCursor
                (tx, CursorConfig.READ_UNCOMMITTED);
            DatabaseEntry key = new DatabaseEntry ();
            DatabaseEntry data = new DatabaseEntry();
            if (OperationStatus.SUCCESS == cursor.getLast(key, data, null)) {
                int cnt = 1;
                do {
                    TripleEntry entry =
                        (TripleEntry) entryBinding.entryToObject(key);
                    Entity.Triple triple = entry.triple;                    
                    Entity source = ef.entity(triple.source());
                    Entity target = ef.entity(triple.target());
                    Map<StitchKey, Object> values = triple.values();

                    logger.info("----- "+cnt+"/"+count
                                +" "+triple.source() +" "+triple.target()
                                +" "+entry.score+" -----\n"
                                + Util.toString(values));
                    
                    double score = entry.score;
                    boolean override = false;
                    int minval = 1;

                    if (values.containsKey(H_LyChI_L5)) {
                        override = true;
                    }
                    else if (values.containsKey(H_LyChI_L4)) {
                        override = checkStructureCompatibility 
                            (values.get(H_LyChI_L4), source, target);
                        if (!override) ++minval;
                    }
                    else if (values.containsKey(H_LyChI_L3)) {
                        // adjust the score to require strong evidence if there
                        // might be a chance of structural problems
                        score /= 3.0; // eh..??
                        logger.warning("** score is adjusted from "
                                       +entry.score+" to "+score);
                        ++minval;
                    }
                    else {
                        ++minval;
                    }
                    
                    if (override || (values.size() > minval
                                     // prevent rouge connection
                                     && score >= thres)) {
                        Long scolor = uf.root(triple.source()), 
                            tcolor = uf.root(triple.target());
                        // now make sure this is compatible with existing
                        // stitch
                        boolean compatible = true;
                        if (scolor != null) {
                            compatible = transitiveStructureCompatibility 
                                (values.get(H_LyChI_L4), source, target);
                        }

                        if (compatible && tcolor != null) {
                            compatible = transitiveStructureCompatibility 
                                (values.get(H_LyChI_L4), target, source);
                        }
                        
                        if (compatible && union (target, source)) {
                            // now let's interogate this a big more 
                            logger.info("## merging ("+scolor+") "
                                        +source.getId()
                                        +" "+target.getId()+" ("+tcolor+").. "
                                        +score +" "+Util.toString(values));
                        }
                    }
                    else {
                        uf.add(source.getId());
                        uf.add(target.getId());
                    }
                    ++cnt;
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
            File file = env.getHome();
            catalog.close();
            tripleDb.close();
            env.close();
            file.delete();
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

            List<Long> comps = new ArrayList<>();
            Double score = null;
            if (argv.length > 2) {
                for (int i = 2; i < argv.length; ++i) {
                    int index = argv[i].indexOf("score=");
                    if (index >= 0) {
                        score = Double.parseDouble(argv[i].substring(index+6));
                    }
                    else {
                        try {
                            long id = Long.parseLong(argv[i]);
                            comps.add(id);
                        }
                        catch (NumberFormatException ex) {
                            logger.warning("Bogus component id: "+argv[i]);
                        }
                    }
                }
            }
            else {
                ef.components(comps);
                logger.info("##### there are "+comps.size()+" components #####");
            }

            if (score != null)
                logger.info("#### MINSCORE = "+score+" #####");

            for (int i = 0; i < comps.size(); ++i) {
                long id = comps.get(i);
                Entity e = ef.entity(id);
                logger.info("################ COMPONENT "
                            +String.format("%1$5d", i+1)+": "+
                            +id+" ("+e.get(RANK)+") ################");
                UntangleCompoundStitches ucs =
                    new UntangleCompoundStitches (dsource, e, score);
                ef.untangle(ucs);
                ucs.shutdown();
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
