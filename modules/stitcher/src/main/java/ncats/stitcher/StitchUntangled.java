package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.function.BiPredicate;

import org.neo4j.graphdb.GraphDatabaseService;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public class StitchUntangled {
    static final Logger logger = Logger.getLogger
        (StitchUntangled.class.getName());

    static class StitchKeyBlacklistPredicate
        implements BiPredicate<StitchKey, Object> {

        final Map<StitchKey, Set> blacklist;
        StitchKeyBlacklistPredicate (StitchKey... keys) {
            blacklist = new HashMap<>();
            for (StitchKey k : keys)
                blacklist.put(k, new HashSet ());
        }

        public StitchKeyBlacklistPredicate blacklist
            (StitchKey key, Object... values) {
            if (!blacklist.containsKey(key))
                throw new IllegalArgumentException (key+" is not available!");
            
            Set set = blacklist.get(key);
            for (Object obj : values)
                set.add(obj);
            
            return this;
        }

        public boolean test (StitchKey key, Object value) {
            Set set = blacklist.get(key);
            return set != null && !set.contains(value);
        }
    }

    final EntityFactory ef;
    public StitchUntangled (GraphDb graphDb) {
        ef = new EntityFactory (graphDb);
    }

    public int untangle (Component component) {
        Util.dump(component);
        UnionFind uf = new UnionFind ();

        Set<Long> promiscous = new TreeSet<>();
        // all priority 5 keys
        for (StitchKey k : EnumSet.allOf(StitchKey.class)) {
            Map<Object, Integer> stats = component.stats(k);
            if (5 == k.priority) {
                for (Map.Entry<Object, Integer> v : stats.entrySet()) {
                    long[] nodes = component.nodes(k, v.getKey());
                    logger.info(k+"="+v.getKey()+" => "+nodes.length);
                    if (nodes.length > 0) {
                        for (int i = 0; i < nodes.length; ++i) {
                            Entity e = ef.entity(nodes[i]);
                            Object val = e.keys().get(k);
                            assert (val != null);
                            System.out.print(i > 0 ? "," : "[");
                            System.out.print(nodes[i]);
                            if (val.getClass().isArray()
                                && Array.getLength(val) > 1) {
                                // this node has more than one stitches
                                promiscous.add(nodes[i]);
                            }
                            else {
                                for (int j = 0; j < i; ++j)
                                    if (!promiscous.contains(nodes[j]))
                                        uf.union(nodes[i], nodes[j]);
                            }
                        }
                        System.out.println("]");
                    }
                }
            }
        }

        /*
        component.cliques(c -> {
                Util.dump(c);
                
                Map<StitchKey, Object> values = c.values();
                // first only consider cliques that span multiple stitches
                boolean ok = true;//values.size() > 1;
                if (!ok) {
                    // only one stitch key
                    StitchKey key = values.keySet().iterator().next();
                    ok = key == StitchKey.H_LyChI_L4
                        || key == StitchKey.H_LyChI_L5;
                }
                
                if (ok) {
                    long[] nodes = c.nodes();
                    for (int i = 0; i < nodes.length; ++i)
                        for (int j = i+1; j < nodes.length; ++j)
                            uf.union(nodes[i], nodes[j]);
                }
                
                return true;
            }, StitchKey.keys(1, -1));
        */
        
        long[][] clumps = uf.components();
        StitchKeyBlacklistPredicate pred = new StitchKeyBlacklistPredicate
            (StitchKey.keys(1, -1));
        pred.blacklist(H_LyChI_L4,
                       "V7HGXMSPT2WU",// 2278
                       "VRNNW5CV2L8L",// 1176
                       "VDTT9FYX4XVV",// 990
                       "VSRJD7T6ZP6D",// 703
                       "VK58UXXX2XJB",// 465
                       "VK52RU38KBRT",// 325
                       "3G5YPMD65QHZ",// 276
                       "VZPBXZ7U7M94")
            .blacklist(T_ActiveMoiety, "9159UV381P") // hydroxy ion
            ;
        
        System.out.println("************** Refined Clusters ****************");
        for (int i = 0; i < clumps.length; ++i) {
            System.out.println((i+1)+" "+clumps[i].length+":");
            for (int j = 0; j < clumps[i].length; ++j) {
                System.out.print("   "+clumps[i][j]);
                long[] ext = ef.neighbors(clumps[i][j]);
                if (ext != null) {
                    System.out.print(" "+ext.length+":");
                    for (int k = 0; k < ext.length; ++k)
                        System.out.print(" "+ext[k]);
                    System.out.println();
                    
                    ef.cliqueEnumeration(ext, c -> {
                            Util.dump(c);
                            return true;
                        }, H_LyChI_L4, H_LyChI_L5, N_Name, I_CAS, I_UNII);
                }
            }
            System.out.println();
        }
        
        System.out.println("************** Promicuous ****************");
        System.out.println(promiscous);
        
        return 0;
    }

    public int untangle (long id) {
        return untangle (ef.component(id));
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+StitchUntangled.class.getName()
                               +" DB COMPONENTS...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            StitchUntangled su = new StitchUntangled (graphDb);
            for (int i = 1; i < argv.length; ++i) {
                su.untangle(Long.parseLong(argv[i]));
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
