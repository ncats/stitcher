package ncats.stitcher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.net.URI;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundStitches extends UntangleCompoundAbstract {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundStitches.class.getName());

    final protected double threshold;
    
    public UntangleCompoundStitches (DataSource dsource, double threshold) {
        super (dsource);
        this.threshold = threshold;
    }

    static double calcScore (Map<Object, Integer> counts,
                             Entity.Triple triple) {
        Map<StitchKey, Object> values = triple.values();        
        double score = 0., total = 0.;
        // this should be calculated once..
        for (Integer v : counts.values())
            total += v;

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

    @Override
    public void untangle (EntityFactory ef, BiConsumer<Long, long[]> consumer) {
        this.ef = ef;
        uf.clear();

        Map<Object, Integer> counts = new HashMap<>();
        logger.info("############## CALCULATING COUNTS ##############");
        ef.traverse((traversal, triple) -> {
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
        logger.info("$$$$ COUNTS ==> "+counts);

        Set<Entity> roots = new HashSet<>();
        Set<Entity> unsure = new HashSet<>();
        ef.traverse((traversal, triple) -> {
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
        List<Map.Entry<Entity.Triple, Double>> seeds = new ArrayList<>();
        ef.traverse((traversal, triple) -> {
                double score = calcScore (counts, triple);
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
                if (score > threshold && !(a && b)) {
                    seeds.add(new AbstractMap.SimpleImmutableEntry
                              (triple, score));
                }
                else {
                    if (!a) uf.add(source.getId());
                    if (!b) uf.add(target.getId());
                }
                
                return true;
            });

        Collections.sort(seeds, (a, b) -> {
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
        
        for (Map.Entry<Entity.Triple, Double> me : seeds) {
            Entity source = me.getKey().source();
            Entity target = me.getKey().target();
            if (union (target, source)) {
                logger.info("## merging "+source.getId()+" "+target.getId()
                            +".. "+me.getValue());
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

            ef.untangle(new UntangleCompoundStitches (dsource, 10.0));
        }
        finally {
            graphDb.shutdown();
        }
    }
}
