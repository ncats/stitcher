package ncats.stitcher;

import java.util.*;
import java.io.*;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundComponent extends UntangleComponent {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundComponent.class.getName());

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

    class CliqueClosure {
        final Entity entity;
        final StitchKey[] keys;
        final Map<StitchKey, Object> values;
        final boolean anyvalue; // single valued?

        Clique bestClique; // best clique so far
        Map<Long, Integer> nodes = new HashMap<>();

        CliqueClosure (Entity entity, StitchKey... keys) {
            this (entity, true, keys);
        }
        
        CliqueClosure (Entity entity, boolean anyvalue, StitchKey... keys) {
            this.entity = entity;
            this.keys = keys;
            this.anyvalue = anyvalue;
            values = entity.keys();
        }

        public boolean closure () {
            for (StitchKey k : keys) {
                Object value = values.get(k);
                if (value != null) {
                    logger.info("***** searching for clique (e="
                                +entity.getId()+") "
                                +k+"="+Util.toString(value));
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        if (anyvalue || len == 1) {
                            for (int i = 0; i < len; ++i) {
                                Object val = Array.get(value, i);
                                findClique (k, val);
                            }
                        }
                    }
                    else {
                        findClique (k, value);
                    }
                }
            }

            // now merge all nodes in the clique!
            if (bestClique != null) {
                long[] nodes = bestClique.nodes();
                for (int i = 1; i < nodes.length; ++i)
                    uf.union(nodes[i], nodes[i-1]);
            }
            else if (!nodes.isEmpty()) {
                // find labeled nodes that maximally span multiple cliques
                Integer max = Collections.max(nodes.values());
                Map<Long, Set<Long>> map = new TreeMap<>();
                for (Map.Entry<Long, Integer> me : nodes.entrySet()) {
                    if (me.getValue() == max) {
                        Long cls = uf.root(me.getKey());
                        Set<Long> set = map.get(cls);
                        if (set == null)
                            map.put(cls, set = new HashSet<>());
                        set.add(me.getKey());
                    }
                }

                logger.info("No best clique found, so resort to "
                            +"maximal clique span heuristic: "+map);
                
                // find label with maximum members
                Set<Long> best = null;
                for (Map.Entry<Long, Set<Long>> me : map.entrySet()) {
                    int size = me.getValue().size();
                    if (best == null || best.size() < size) {
                        best = me.getValue();
                    }
                    else if (best.size() == size)
                        return false; // not unanimous
                }

                logger.info("## resolving "+entity.getId()+" => "+best);
                for (Long n : best)
                    uf.union(entity.getId(), n);
            }

            return !nodes.isEmpty();
        }

        boolean containsExactly (Component comp, StitchKey key, Object value) {
            for (Entity e : comp) {
                Object dif = Util.delta(e.get(key), value);
                if (dif == Util.NO_CHANGE) {
                    return false;
                }
                else if (dif != null) {
                    switch (key) {
                    case H_LyChI_L3:
                    case H_LyChI_L4:
                    case H_LyChI_L5:
                        // if the remaining entries are salt/solvent, then
                        // it's still ok?
                        for (int i = 0; i < Array.getLength(dif); ++i) {
                            String v = (String) Array.get(dif, i);
                            if (v.endsWith("-N") || v.endsWith("-M"))
                                return false;
                        }
                        break;
                        
                    default:
                        return false;
                    }
                }
            }
            return true;
        }

        void findClique (StitchKey key, Object value) {
            Integer count = stats.get(key).get(value);
            component.cliques(clique -> {
                    logger.info("$$ found clique "+key+"="
                                +Util.toString(value)
                                +" ("+count+") => ");
                    Util.dump(clique);
                    
                    if (count == (clique.size() * (clique.size() -1)/2)) {
                        // collapse components
                        if (clique.contains(entity)) {
                            Map<Long, Integer> classes = new HashMap<>();
                            int unmapped = 0;
                            for (Long n : clique.nodeSet()) {
                                Long c = uf.root(n);
                                logger.info(" .. "+n+" => "+ c);
                                if (c != null) {
                                    Integer cnt = classes.get(c);
                                    classes.put(c, cnt == null ? 1 :cnt+1);
                                    // keeping track of labeled nodes in cliques
                                    cnt = nodes.get(n);
                                    nodes.put(n, cnt == null ? 1 : cnt+1);
                                }
                                else
                                    ++unmapped;
                            }
                            
                            if (classes.size() < 2
                                && (anyvalue
                                    || containsExactly (clique, key, value))
                                && (bestClique == null
                                    || bestClique.size() < clique.size())) {
                                bestClique = clique;
                                logger.info("## best clique updated: "
                                            +clique.getId()+" key="+key
                                            +" value="+Util.toString(value));
                            }
                        }
                    }
                    else {
                        logger.warning("** might be spurious clique **");
                    }
            
                    return true;
                }, key, value);
        }
    } // CliqueClosure
    
    public UntangleCompoundComponent (DataSource dsource,
                                      Component component) {
        super (dsource, component);
    }

    @Override
    public void untangle (BiConsumer<Long, long[]> consumer) {
        System.out.println("## Active moieties for component ##");
        System.out.println(component.nodeSet());

        // collapse based on single/terminal active moieties
        Set<Entity> unsure = new TreeSet<>();
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
        //dumpActiveMoieties ();
        dump ("Active moiety stitching");

        // collapse based on trusted stitch keys; e.g., lychi layer 5, unii
        component.stitches((source, target) -> {
                uf.union(source.getId(), target.getId());
            }, H_LyChI_L5, I_UNII);
        dump ("trusted keys stitching");

        int count = 0;        
        // now find all remaining unmapped nodes
        int processed = 0, total = component.size();
        for (Entity e : component) {
            if (unsure.contains(e)) {
            }
            else if (!uf.contains(e.getId())) {
                System.out.println("++++++++++++++ " +processed
                                   +"/"+total+" ++++++++++++++");
                if (transitive (e, H_LyChI_L4)) {
                    // 
                }
                else if (clique (e, N_Name, I_CAS)) {
                }
                else if (clique (e, false, H_LyChI_L3)) {
                    // desparate, last resort but require large min clique
                }
                else {
                    uf.add(e.getId());
                    logger.warning("** unmapped entity "+e.getId()+": "
                                   +e.keys());
                    
                    ++count;
                }
            }
            ++processed;
        }
        //dump ("handle unmapped nodes");

        // now handle unresolved nodes with multiple active moieties and
        // assign to the class with less references 
        for (Entity e : unsure) {
            Entity[] nb = e.outNeighbors(T_ActiveMoiety);
            if (nb.length > 1 && !uf.contains(e.getId())) {
                Map<Long, Integer> votes = new HashMap<>();
                for (Entity u : nb) {
                    for (Object v : Util.toSet(u.get(H_LyChI_L4))) {
                        if (v.toString().endsWith("-M")
                            || v.toString().endsWith("-N")) {
                            Integer c = votes.get(u.getId());
                            votes.put(u.getId(), c==null?1:c+1);
                        }
                    }
                }

                if (votes.size() == 1) {
                    Long id = votes.keySet().iterator().next();
                    uf.union(e.getId(), id);
                }
                else {
                    uf.add(e.getId()); // its own component
                }
            }
        }
        dump ("nodes with multiple active moieties");
        
        // now generate untangled compoennts..
        for (long[] comp : uf.components()) {
            consumer.accept(getRoot (comp), comp);
        }
    }

    /*
     * TODO: find the root active moiety and if exists return it
     */
    Long getRoot (long[] comp) {
        Entity[] entities = component.entities(comp);
        if (entities.length != comp.length)
            logger.warning("There are missing entities in component!");
        
        for (Entity e : entities) {
            Entity[] in = e.inNeighbors(T_ActiveMoiety);
            Entity[] out = e.outNeighbors(T_ActiveMoiety);
            
            if (in.length > 0 && out.length == 0)
                return e.getId();
        }
        
        return null;
    }

    boolean transitive (Entity e, StitchKey key) {
        return new TransitiveClosure(e, key).closure();
    }

    boolean transitive (Entity e, Entity[] nb, StitchKey key) {
        return new TransitiveClosure(e, nb, key).closure();
    }

    boolean clique (Entity e, StitchKey... keys) {
        return new CliqueClosure(e, keys).closure();
    }
    
    boolean clique (Entity e, boolean anyvalue, StitchKey... keys) {
        return new CliqueClosure(e, anyvalue, keys).closure();
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

    static void dumpComponents (EntityFactory ef) throws IOException {
        PrintStream ps = new PrintStream
            (new FileOutputStream ("components.txt"));
        Map<Integer, Integer> hist = new TreeMap<>();
        ef.components(component -> {
                Entity root = component.root();
                Integer rank = (Integer) root.get(Props.RANK);
                ps.println(root.getId()+"\t"+rank);
                Integer c = hist.get(rank);
                hist.put(rank, c == null ? 1 : c+1);
            });
        ps.close();
        
        System.out.println("Component rank histogram:");
        for (Map.Entry<Integer, Integer> me : hist.entrySet()) {
            System.out.println(me.getKey()+"\t"+me.getValue());
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundComponent.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }
        
        Integer version = 0;
        try {
            version = Integer.parseInt(argv[1]);
            if (version == 0) {
                System.err.println("VERSION can't be 0");
                System.exit(1);
            }
        }
        catch (NumberFormatException ex) {
            System.err.println("VERSION must be numerical, e.g., 1, 2,...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            //dumpComponents (ef);

            DataSource dsource =
                ef.getDataSourceFactory().register("stitch_v"+version);
            
            if (argv.length == 2) {
                // do all components
                logger.info("Untangle all components...");
                List<Long> components = new ArrayList<>();
                ef.components(component -> {
                        /*
                        logger.info("Component "+component.getId()+"...");
                        ef.untangle(new UntangleCompoundComponent
                                    (dsource, component));
                        */
                        components.add(component.root().getId());
                    });
                logger.info("### "+components.size()+" components!");
                for (Long cid : components) {
                    logger.info("########### Untangle component "+cid+"...");
                    ef.untangle(new UntangleCompoundComponent
                                (dsource, ef.component(cid)));
                }
            }
            else {
                for (int i = 2; i < argv.length; ++i) {
                    Component comp = ef.component(Long.parseLong(argv[i]));
                    logger.info("Dumping component "+comp.getId());         
                    FileOutputStream fos = new FileOutputStream
                        ("Component_"+comp.getId()+".txt");
                    Util.dump(fos, comp);
                    fos.close();
                    
                    logger.info("Stitching component "+comp.getId());
                    ef.untangle(new UntangleCompoundComponent (dsource, comp));
                }
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
