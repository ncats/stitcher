package ncats.stitcher;

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

public class UntangleCompoundStitches extends UntangleCompoundAbstract {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundStitches.class.getName());

    final protected Double threshold;
    final protected Map<Object, Integer> counts = new HashMap<>();
    final protected Entity seed;
    protected double total;

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
    }

    double calcScore (Entity.Triple triple) {
        Map<StitchKey, Object> values = triple.values();        
        double score = 0.;

        Entity target = triple.target();
        Entity source = triple.source();
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
                Entity source = triple.source(T_ActiveMoiety);
                Entity target = triple.target(T_ActiveMoiety);
                
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
        Queue<Map.Entry<Entity.Triple, Double>> seeds =
            new PriorityQueue<>((a, b) -> {
                    if (b.getValue() > a.getValue()) return 1;
                    if (b.getValue() < a.getValue()) return -1;
                    if (a.getKey().source().getId()
                        < b.getKey().source().getId())
                        return -1;
                    if (a.getKey().source().getId()
                        > b.getKey().source().getId())
                        return 1;
                    return 0;
                });

        List<Double> scores = new ArrayList<>();
        traverse ((traversal, triple) -> {
                double score = calcScore (triple);
                Entity source = triple.source();
                Entity target = triple.target();
                
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
                scores.add(score);
                if (/*score > threshold &&*/!(a && b)) {
                    seeds.add(new AbstractMap.SimpleImmutableEntry
                              (triple, score));
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
        int size = scores.size();       
        if (size > 0) {
            Collections.sort(scores);
            double min = scores.get(0);
            double max = scores.get(scores.size()-1);
            
            double med = 0.;
            if (size % 2 == 0)
                med = (scores.get(size/2) + scores.get(size/2 -1))/2.0;
            else
                med = scores.get(size/2);

            double mean = 0.;
            for (Double s : scores)
                mean += s;
            mean /= size;
            
            logger.info("$$$$$$ SCORE: MIN = "+min
                        +" MAX = "+max
                        +" MEDIAN = "+med
                        +" MEAN = "+mean+" $$$$$$$$");
            if (min < 1.)
                thres = Math.max(5.0*min, mean - 5.0*min); // eh..??
            else
                thres = min;
        }

        if (threshold != null)
            thres = threshold;

        logger.info("$$$$$$ THRESHOLD = "+thres+" ");

        for (Map.Entry<Entity.Triple, Double> me;
             (me = seeds.poll()) != null; ) {
            Entity.Triple triple = me.getKey();
            Entity source = triple.source();
            Entity target = triple.target();
            
            double score = me.getValue();
            if (score > thres && triple.values().containsKey(H_LyChI_L3)
                && !triple.values().containsKey(H_LyChI_L4)) {
                // adjust the score to require strong evidence if there
                // might be a chance of structural problems
                score /= 3.0; // eh..??
                logger.warning("** score is adjusted from "
                               +me.getValue()+" to "+score);
            }
            
            if (score > thres && union (target, source)) {
                // now let's interogate this a big more                
                logger.info("## merging "+source.getId()+" "+target.getId()
                            +".. "+me.getValue());
            }
            else {
                uf.add(source.getId());
                uf.add(target.getId());
            }
        }

        // now resolve entities that point to multiple active moieties
        for (Entity e : unsure) {
            Entity[] nb = e.outNeighbors(T_ActiveMoiety);
            // just assign to arbitrary or based on rank?
            uf.union(e.getId(), nb[0].getId());
        }

        createStitches (consumer);
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
                    ef.untangle(new UntangleCompoundStitches (dsource, e));
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
                    ef.untangle(new UntangleCompoundStitches (dsource, e));
                }
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
