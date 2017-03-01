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
        // first only consider priority 5 keys
        for (StitchKey k : StitchKey.keys(5, 5)) {
            Map<Object, Integer> stats = component.stats(k);
            for (Map.Entry<Object, Integer> v : stats.entrySet()) {
                long[] nodes = component.nodes(k, v.getKey());

                logger.info(k+"="+v.getKey()+" => "+nodes.length);
                if (nodes.length > 0) {
                    int union = 0;
                    for (int i = 0; i < nodes.length; ++i) {
                        Entity e = ef.entity(nodes[i]);
                        Object val = e.keys().get(k);
                        assert (val != null);
                        System.out.print(i > 0 ? "," : "[");
                        System.out.print(nodes[i]);
                        if (val.getClass().isArray()
                            && Array.getLength(val) > 1) {
                            // this node has more than one stitches, so let's
                            // deal with it later
                            promiscous.add(nodes[i]);
                        }
                        else {
                            uf.add(nodes[i]);
                            for (int j = 0; j < i; ++j)
                                if (!promiscous.contains(nodes[j]))
                                    uf.union(nodes[i], nodes[j]);
                        }
                    }
                    System.out.println("]");
                }
            }
        }
        
        long[][] clumps = uf.components();
        
        System.out.println("************** 1. Refined Clusters ****************");
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+": ");
            for (int j = 0; j < clumps[i].length; ++j) {
                System.out.print((j==0 ? "[":",")+clumps[i][j]);
                Map<StitchValue, long[]> extent = ef.expand(clumps[i][j]);
                for (Map.Entry<StitchValue, long[]> me : extent.entrySet()) {
                    StitchKey key = me.getKey().getKey();
                    if (key.priority > 0) {
                        long[] nodes = me.getValue();
                        for (int k = 0; k < nodes.length; ++k)
                            if (!uf.contains(nodes[k]))
                                uf.union(clumps[i][j], nodes[k]);
                    }
                }
            }
            System.out.println("]");
        }

        // now assign promiscous nodes
        System.out.println("************** 2. Refined Clusters ****************");
        clumps = uf.components();
        long[] nodes = Util.toArray(promiscous);
        
        List<Component> components = new ArrayList<>();
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+": ");
            for (int j = 0; j < clumps[i].length; ++j)
                System.out.print((j==0 ? "[" : ",")+clumps[i][j]);
            System.out.println("]");
            
            Component comp = ef.component(clumps[i]);
            components.add(comp.add(nodes, StitchKey.keys(5, 5)));
        }

        Set<Long> leftover = new TreeSet<>(component.nodeSet());
        for (Component c : components) {
            promiscous.removeAll(c.nodeSet());
            leftover.removeAll(c.nodeSet());
        }
        
        System.out.println("************** Promicuous ****************");
        System.out.println(promiscous);

        System.out.println("************** Leftover Nodes ******************");
        System.out.println(leftover);
        for (Long n : leftover) {
            System.out.print("+"+n+":");
            component.depthFirst(n, p -> {
                    System.out.print("<");
                    for (int i = 0; i < p.length; ++i)
                        System.out.print((i==0?"":",")+p[i]);
                    System.out.print(">");
                }, H_LyChI_L4, N_Name);
            System.out.println();   
        }
        if (!leftover.isEmpty()) {
            Component c = ef.component(Util.toArray(leftover));
            Util.dump(c);
            for (StitchKey k : EnumSet.of(N_Name, H_LyChI_L4)) {
                for (Object v : c.values(k)) {
                    System.out.println("   >>> cliques "+k+"="+v+" <<<");
                    c.cliques(clique -> {
                            Util.dump(clique);
                            return true;
                        }, k, v);
                }
            }
        }

        System.out.println("************** Final Components ****************");
        for (Component c : components) {
            Util.dump(c);
        }
        
        System.out.println("####### "+components.size()+" components! #######");
        
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
