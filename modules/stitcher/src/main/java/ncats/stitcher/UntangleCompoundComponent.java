package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundComponent {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundComponent.class.getName());

    final Component component;
    final UnionFind uf = new UnionFind ();
    final Map<StitchKey, Map<Object, Integer>> stats;
    final Map<Object, Set<Entity>> moieties = new TreeMap<>();

    class TransitiveClosure {
        Entity emin;
        Object vmin;
        Integer cmin;
        
        final Map<Object, Integer> counts;
        final Entity e;
        final Entity[] nb;
        final StitchKey key;
        final Object kval;
        
        TransitiveClosure (Entity e, StitchKey key) {
            this (e, e.neighbors(key), key);
        }
        
        TransitiveClosure (Entity e, Entity[] nb, StitchKey key) {
            this.e = e;
            this.nb = nb;
            this.key = key;
            
            kval = e.get(key);
            counts = stats.get(key);
        }

        boolean checkNoExt (Object value, Set except, String ext) {
            Set set = Util.toSet(value);
            if (except != null)
                set.removeAll(except);
            
            for (Object sv : set) {
                if (!sv.toString().endsWith(ext))
                    return false;
            }
            return true;
        }

        boolean checkExt (Object value, String ext) {
            Set set = Util.toSet(value);
            for (Object sv : set) {
                if (sv.toString().endsWith(ext))
                    return true;
            }
            return false;
        }

        boolean checkActiveMoiety (Entity u, Object value) {
            Set<Entity> su = moieties.get
                (valueToString (u.get(H_LyChI_L4), ':'));
            if (su != null) {
                Set<Entity> v = moieties.get(valueToString (value, ':'));
                if (v != null)
                    for (Entity z : v)
                        if (su.contains(z))
                            return true;
                
                // try individual values?
                if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); ++i) {
                        v = moieties.get
                            (valueToString(Array.get(value, i), ':'));
                        if (v != null && su.containsAll(v))
                            return true;
                    }
                }
            }
            
            return su == null;
        }

        void updateIfCompatible (Entity u, Object v) {
            Integer c = null;
            if (v.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(v); ++i) {
                    Integer n = counts.get(Array.get(v, i));
                    if (c == null || n < c)
                        c = n;
                }
            }
            else
                c = counts.get(v);

            boolean hasMetal = false;
            Set vset = Util.toSet(v);
            for (Object sv : vset) {
                if (sv.toString().endsWith("-M")) {
                    hasMetal = true;
                    break;
                }
            }
            
            if (cmin == null
                || (cmin > c && !checkExt (vmin, "-M")) || hasMetal) {
                boolean update = checkNoExt (u.get(key), vset, "-S")
                    && checkNoExt (kval, vset, "-S")
                    && checkActiveMoiety (u, v);
                
                if (update) {
                    emin = u;
                    cmin = c;
                    vmin = v;
                }
            }
        }
        
        public boolean closure () {
            for (Entity u : nb) {
                Object value = e.keys(u).get(key);
                if (value != null) {
                    updateIfCompatible (u, value);
                }
            }
        
            // now merge
            boolean ok = false;
            if (emin != null) {
                if (uf.contains(emin.getId())) {
                    if (cmin < 1000) {
                        logger.info(".."+e.getId()+" <-["+key+"="
                                    +vmin+":"+cmin+"]-> "+emin.getId());
                        uf.union(e.getId(), emin.getId());
                        ok = true;
                    }
                }
                else {
                    logger.info(".."+e.getId()+" <-["+key+"="
                                +vmin+":"+cmin+"]-> "+emin.getId());
                    uf.union(e.getId(), emin.getId());
                    ok = true;
                }
            }
            
            return ok;
        } // closure
    } // TransitiveClosure
    
    public UntangleCompoundComponent (Component component) {
        stats = new HashMap<>();
        for (StitchKey key : Entity.KEYS) {
            stats.put(key, component.stats(key));
        }
        this.component = component;
    }

    static String valueToString (Object value, char sep) {
        StringBuilder sb = new StringBuilder ();
        if (value.getClass().isArray()) {
            String[] ary = new String[Array.getLength(value)];
            for (int i = 0; i < ary.length; ++i)
                ary[i] = Array.get(value, i).toString();

            Arrays.sort(ary);
            for (int i = 0; i < ary.length; ++i) {
                if (sb.length() > 0)
                    sb.append(sep);
                sb.append(ary[i]);
            }
        }
        else {
            sb.append(value.toString());
        }

        return sep == ',' ? "["+sb+"]" : sb.toString();
    }

    public void untangle () {
        System.out.println("## Active moieties for component ##");
        System.out.println(component.nodeSet());

        // collapse based on single/terminal active moieties
        List<Entity> unsure = new ArrayList<>();
        component.stitches((source, target) -> {
                Entity[] out = source.outNeighbors(T_ActiveMoiety);
                Entity[] in = target.inNeighbors(T_ActiveMoiety);
                System.out.println(" ("+out.length+") "+source.getId()
                                   +" -> "+target.getId()+" ("+in.length+")");
                if (out.length == 1) {
                    // first collapse single/terminal active moieties
                    uf.union(source.getId(), target.getId());
                }
                else if (out.length > 1) {
                    unsure.add(source);
                }

                for (Entity e : in) {
                    Object value = e.get(H_LyChI_L4.name());
                    if (value != null) {
                        String key = valueToString (value, ':');
                        Set<Entity> set = moieties.get(key);
                        if (set == null)
                            moieties.put(key, set = new TreeSet<>());
                        set.add(target);
                    }
                    else {
                        logger.warning("** No "+H_LyChI_L4
                                       +" value for "+e.getId());
                    }
                }
                
                Object value = target.get(H_LyChI_L4.name());
                if (value != null) {
                    String key = valueToString (value, ':');
                    Set<Entity> set = moieties.get(key);
                    if (set == null)
                        moieties.put(key, set = new TreeSet<>());
                    set.add(target);
                }
            }, T_ActiveMoiety);
        dumpActiveMoieties ();
        dump ("Active moiety stitching");

        // collapse based on lychi layer 5
        component.stitches((source, target) -> {
                uf.union(source.getId(), target.getId());
            }, H_LyChI_L5);
        dump (H_LyChI_L5+" stitching");

        int count = 0;
        for (Entity e : unsure) {
            Entity[] nb = e.outNeighbors(T_ActiveMoiety);
            assert nb.length > 1: "Expecting active moiety "
                +"neighbors > 1 but instead got "+nb.length+"!";
            
            Entity u = null;
            Integer best = null;
            for (Entity n : nb) {
                Object kv = e.keys(n).get(T_ActiveMoiety);
                Integer c = stats.get(T_ActiveMoiety).get(kv);
                if (u == null || best > c) {
                    u = n;
                    best = c;
                }
            }
            
            if (u != null) {
                uf.union(e.getId(), u.getId());
            }
            else {
                logger.warning("** unmapped entity "+e.getId()+": "
                               +e.keys());
                ++count;
            }
        }
        dump ("Stitching "+unsure.size()
              +" nodes with multiple active moieties");

        // now find all remaining unmapped nodes
        int processed = 0, total = component.size();
        for (Entity e : component) {
            if (!uf.contains(e.getId())) {
                System.out.println("++++++++++++++ " +processed
                                   +"/"+total+" ++++++++++++++");
                if (!closure (e, H_LyChI_L4)) {
                    logger.warning("** unmapped entity "+e.getId()+": "
                                   +e.keys());
                    ++count;
                }
            }
            ++processed;
        }
        dump ("unmapped nodes");

        logger.info("### "+count+" unstitched entities!");
    }

    boolean closure (Entity e, StitchKey key) {
        return new TransitiveClosure(e, key).closure();
    }

    boolean closure (Entity e, Entity[] nb, StitchKey key) {
        return new TransitiveClosure (e, nb, key).closure();
    }

    void dump (String mesg) {
        long[][] components = uf.components();
        System.out.println("** "+mesg+": number of components: "
                           +components.length);
        for (long[] c : components) {
            System.out.print(c.length+" [");
            for (int i = 0; i < c.length; ++i) {
                System.out.print(c[i]);
                if (i+1 < c.length)
                    System.out.print(",");
            }
            System.out.println("]");
        }
    }

    void dumpActiveMoieties () {
        try {
            FileOutputStream fos = new FileOutputStream
                ("Component_"+component.getId()+"_activemoieties.txt");
            PrintStream ps = new PrintStream (fos);
            for (Map.Entry<Object, Set<Entity>> me : moieties.entrySet()) {
                ps.print(me.getKey());
                for (Entity e : me.getValue())
                    ps.print(" "+e.getId());
                ps.println();
            }
            ps.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundComponent.class.getName()
                               +" DB COMPONENTS...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);         
            for (int i = 1; i < argv.length; ++i) {
                Component comp = ef.component(Long.parseLong(argv[i]));
                logger.info("Dumping component "+comp.getId());         
                FileOutputStream fos = new FileOutputStream
                    ("Component_"+comp.getId()+".txt");
                Util.dump(fos, comp);
                fos.close();
                
                logger.info("Stitching component "+comp.getId());
                UntangleCompoundComponent ucc =
                    new UntangleCompoundComponent (comp);
                ucc.untangle();
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
