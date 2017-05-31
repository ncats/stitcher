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

    public void untangleCompounds (Component component) {
        new UntangleCompoundComponent(component).untangle();
    }
    
    public void untangleCompounds (long id) {
        untangleCompounds (ef.component(id));
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

        Map<Long, Set<String>> colors = new TreeMap<>();
        List<Component> components = new ArrayList<>();
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+": ");
            for (int j = 0; j < clumps[i].length; ++j)
                System.out.print((j==0 ? "[" : ",")+clumps[i][j]);
            System.out.println("]");
            
            Component comp = ef.component(clumps[i]);
            comp = comp.add(nodes, StitchKey.keys(5, 5));
            
            long[] nc = comp.nodes();
            for (int j = 0; j < nc.length; ++j) {
                Set<String> c = colors.get(nc[j]);
                if (c == null)
                    colors.put(nc[j], c = new HashSet<>());
                c.add(comp.getId());
            }
                
            components.add(comp);
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
            
            Map<Long, Set<Long>> cands = new TreeMap<>();
            for (StitchKey k : new StitchKey[]{
                    // in order of high to low priority
                    H_LyChI_L4, I_UNII, I_CAS,
                    N_Name, H_LyChI_L3
                }) {
                for (Object v : c.values(k)) {
                    c.cliques(clique -> {
                            System.out.println
                                (">>> cliques for "+k+"="+v+" <<<");
                            long[] nc = clique.nodes();
                            Set<Long> unmapped = new TreeSet<>();
                            Set<Long> mapped = new TreeSet<>();
                            for (int i = 0; i < nc.length; ++i) {
                                System.out.print(" "+nc[i]);
                                Set<String> s = colors.get(nc[i]);
                                if (s != null) {
                                    Iterator<String> it = s.iterator();
                                    System.out.print("["+it.next());
                                    while (it.hasNext()) {
                                        System.out.print(","+it.next());
                                    }
                                    System.out.print("]");
                                    mapped.add(nc[i]);
                                }
                                else {
                                    unmapped.add(nc[i]);
                                }
                            }
                            System.out.println();

                            if (mapped.isEmpty()) {
                                // create a new component
                                Component comp = ef.component(nc);
                                Set<String> s =
                                    Collections.singleton(comp.getId());
                                for (int i = 0; i < nc.length; ++i)
                                    colors.put(nc[i], s);
                            }
                            else {
                                // now assign each unmapped node to the best
                                // component
                                for (Long n : unmapped) {
                                    Set<Long> m = cands.get(n);
                                    if (m != null)
                                        m.addAll(mapped);
                                    else
                                        cands.put(n, mapped);
                                }
                            }
                            
                            return true;
                        }, k, v);
                }
            }

            System.out.println("************** Candidate Mapping ****************");
            for (Map.Entry<Long, Set<Long>> me : cands.entrySet()) {
                System.out.print(me.getKey()+":");
                for (Long n : me.getValue()) {
                    System.out.print(" "+n);
                    Set<String> s = colors.get(n);
                    if (s != null) {
                        Iterator<String> it = s.iterator();
                        System.out.print("["+it.next());
                        while (it.hasNext()) {
                            System.out.print(","+it.next());
                        }
                        System.out.print("]");
                    }
                }
                System.out.println();
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

    public void anyPath (long start, long end) {
        Entity s = ef.entity(start);
        Entity e = ef.entity(end);
        if (s != null && e != null) {
            Entity[] path = s.anyPath(e, StitchKey.T_ActiveMoiety,
                                      StitchKey.H_LyChI_L4);
            System.out.print(path.length+" [");
            for (int i = 0; i < path.length; ++i) {
                System.out.print(path[i].getId());
                if (i+1 < path.length) System.out.print(",");
            }
            System.out.println("]");
        }
    }

    public void dump (long id) {
        Util.dump(ef.component(id));
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
                su.untangleCompounds(Long.parseLong(argv[i]));
                //su.dump(Long.parseLong(argv[i]));
            }
            //su.anyPath(387861l, 684375l);
        }
        finally {
            graphDb.shutdown();
        }
    }
}
