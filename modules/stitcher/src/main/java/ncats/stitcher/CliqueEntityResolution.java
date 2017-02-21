package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ncats.stitcher.graph.UnionFind;

public class CliqueEntityResolution implements EntityResolution, CliqueVisitor {
    static final Logger logger =
        Logger.getLogger(CliqueEntityResolution.class.getName());

    /**
     * Don't include L3 hash
     */
    static final StitchKey[] KEYS = EnumSet.complementOf
        (EnumSet.of(StitchKey.H_LyChI_L1, StitchKey.H_LyChI_L2,
                    StitchKey.H_LyChI_L3)).toArray(new StitchKey[0]);

    static boolean DEBUG = false;

    class Expander extends AbstractEntityVisitor {
        public Entity start;
        StitchKey key;
        
        Expander (StitchKey key, Object value) {
            set (key, value);
            this.key = key;
        }

        @Override
        public boolean visit (Entity[] path, Entity e) {
            if (DEBUG) {
                System.out.print("  ");
                for (int i = 0; i < path.length; ++i)
                    System.out.print(" ");
                System.out.println(" + "+e.getId());
            }
            
            eqv.union(start.getId(), e.getId());
            return true;
        }
    }

    static class CliqueComponent implements Component {
        double score;
        Set<Long> nodes = new TreeSet<Long>();
        Entity[] entities;
        String id;
        
        CliqueComponent (double score, Entity... entities) {
            this.score = score;
            for (Entity e : entities) {
                nodes.add(e.getId());
            }
            
            id = Util.sha1(nodes).substring(0, 9);    
            this.entities = entities;
        }

        public String getId() { return id; }
        public Entity[] entities () { return entities; }
        public int size () { return entities.length; }
        public Set<Long> nodeSet () { return nodes; }
        public int hashCode () { return nodes.hashCode(); }
        public boolean equals (Object obj) {
            if (obj instanceof Component) {
                return nodes.equals(((Component)obj).nodes());
            }
            return false;
        }

        public Iterator<Entity> iterator () {
            return Arrays.asList(entities).iterator();
        }
        
        @Override
        public Double score () { return score; }
    }

    EntityFactory ef;
    List<Clique> cliques = new ArrayList<Clique>();
    UnionFind eqv = new UnionFind ();
    List<Long> singletons = new ArrayList<Long>();
    Map<Entity, Set<Clique>> entities = new HashMap<Entity, Set<Clique>>();

    public CliqueEntityResolution () {
    }

    /**
     * EntityResolution interface
     */
    public void resolve (EntityFactory ef, Consumer<Component> consumer) {
        this.ef = ef;

        cliques.clear();
        Set<DataSource> sources =
            DataSourceFactory.getDataSources(ef.getGraphDb());
        
        ef.components(comp -> {
                clear ();               
                Entity[] entities = comp.entities();
                if (entities.length > 1) {
                    if (DEBUG) {
                        logger.info(">>>>> cliques for "
                                    +"component of size "+comp.size()+"...");
                    }
                    long start = System.currentTimeMillis();
                    ef.cliqueEnumeration(KEYS, entities, this);
                    if (DEBUG)
                        logger.info(cliques.size()+" clique(s) found!");

                    /*
                    closure (Arrays.asList(entities).iterator());
                    
                    if (DEBUG) {
                        double elapsed =
                            (System.currentTimeMillis()-start)*1e-3;
                        logger.info
                            ("<<<<< Elapsed time for clique enumeration: "
                             +String.format("%1$.3fs", elapsed));
                    }
                    
                    for (long[] group : eqv.components()) {
                        Map<DataSource, Integer> counts =
                            new HashMap<DataSource, Integer>();
                        for (int i = 0; i < group.length; ++i) {
                            DataSource ds = ef.entity(group[i]).datasource();
                            Integer c = counts.get(ds);
                            counts.put(ds, c!=null ? c+1:1);
                        }
                        
                        double sum = 0.;
                        for (Integer c : counts.values())
                            sum += 1./c;
                        consumer.accept
                            (new CliqueComponent (sum/sources.size(),
                                                  ef.entities(group)));
                    }
                    
                    for (Long id : singletons)
                        consumer.accept
                            (new CliqueComponent
                             (1, ef.entities(new long[]{id})));
                    */
                }
                else {
                    //consumer.accept(new CliqueComponent (1, entities));
                }
            });
        
        //debug ();
        debug2();
    }

    void clear () {
        eqv.clear();
        singletons.clear();
    }

    void closure (Iterator<Entity> iter) { // transitive closure
        while (iter.hasNext()) {
            Entity e = iter.next();
            long id = e.getId();
            if (!eqv.contains(id)) {
                // node not merged, so we try to assign it to one of the
                // best available neighbors

                Long mapped = null;
                int unmapped = 0, max = 0;
                Map<Long, Integer> counts = new HashMap<Long, Integer>();
                for (Entity ne : e.neighbors()) {
                    Long r = eqv.root(ne.getId());
                    if (r != null) {
                        Integer c = counts.get(r);
                        c = c == null ? 1 : c+1;
                        counts.put(r, c);
                        if (c > max) {
                            mapped = r;
                            max = c;
                        }
                    }
                    else {
                        ++unmapped;
                        eqv.union(id, ne.getId());
                    }
                }
                
                if (unmapped > max) {
                    logger.warning("Entity "+id
                                   +" has more unmapped ("+unmapped
                                   +") neighbors than mapped ("+max+")!");
                }

                if (false && max > 0) {
                    //logger.info("** mapping entity "+id+" to "+mapped);
                    eqv.union(mapped, id);
                }
                else if (unmapped == 0) {
                    singletons.add(id);
                }
            }
        }
    }

    void debug () {
        try {
            PrintStream debug = new PrintStream
                (new FileOutputStream ("cliques.txt"));
        
            int n = 1;
            for (Clique c : cliques) {
                debug.println("+++++ clique "+n
                              +" ("+c.size()+") ++++++");
                debug.print("nodes:");
                for (Entity e : c)
                    debug.print(" "+e.getId());
                debug.println();
                debug.print("keys:");
                for (Map.Entry<StitchKey, Object> me : c.values().entrySet())
                    debug.print(" "+me.getKey()+"="
                                +Util.toString(me.getValue(), 0));
                debug.println("\n");
                
                ++n;
            }
            
            debug.close();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void debug2 () {
        try {
            PrintStream ps = new PrintStream
                (new FileOutputStream ("zzentities.txt"));
            Set<Entity> order = new TreeSet<Entity>(new Comparator<Entity> () {
                    public int compare (Entity e1, Entity e2) {
                        Set<Clique> c1 = entities.get(e1);
                        Set<Clique> c2 = entities.get(e2);
                        int d = c2.size() - c1.size();
                        if (d == 0) {
                            long dif = (e1.getId() - e2.getId());
                            if (dif < 0) d = -1;
                            else if (dif > 0) d = 1;
                        }
                        return d;
                    }
                });
            order.addAll(entities.keySet());
            
            for (Entity ent : order) {
                Set<Clique> cliques = entities.get(ent);
                ps.println("["+ent.getId()+" "+cliques.size()+"]");
                for (Clique c : cliques) {
                    ps.print("  nodes: [");
                    for (Entity e : c)
                        ps.print(e.getId()+",");
                    ps.println("]");
                    ps.print("  keys:");
                    for (Map.Entry<StitchKey, Object> me
                             : c.values().entrySet())
                        ps.print(" "+me.getKey()+"="
                                 +Util.toString(me.getValue(), 0));
                    ps.println("\n");
                }
            }
            
            ps.close();

            PrintStream zz = new PrintStream
                (new FileOutputStream ("zzcliques.txt"));

            Collections.sort(cliques, new Comparator<Clique> () {
                    public int compare (Clique c1, Clique c2) {
                        double s = c2.score() - c1.score();
                        if (s < 0.) return -1;
                        if (s > 0.) return 1;
                        int d = c2.values().size() - c1.values().size();
                        if (d == 0)
                            d = c2.size() - c1.size();
                        return d;
                    }
                });

            PrintStream yy = new PrintStream
                (new FileOutputStream ("yycliques.txt"));
            
            int n = 1, m = 1;
            for (Clique c : cliques) {
                int max = 0;
                for (Entity e : c) {
                    Set<Clique> set = entities.get(e);
                    if (set.size() > max) {
                        max = set.size();
                    }
                }
                
                if (max == 1) {
                    zz.println("+++++ clique "+n
                               +" ("+c.size()
                               +String.format(" %1$.3f) ++++++", c.score()));
                    zz.print("nodes:");
                    for (Entity e : c)
                        zz.print(" "+e.getId());
                    zz.println();
                    zz.print("keys:");
                    for (Map.Entry<StitchKey, Object> me
                             : c.values().entrySet())
                        zz.print(" "+me.getKey()+"="
                                 +Util.toString(me.getValue(), 0));
                    zz.println("\n");
                    ++n;
                }
                else {
                    yy.println("+++++ clique "+m
                               +" ("+c.size()
                               +String.format(" %1$.3f) ++++++", c.score()));
                    yy.print("nodes:");
                    for (Entity e : c)
                        yy.print(" "+e.getId()+"("+entities.get(e).size()+")");
                    yy.println();
                    yy.print("keys:");
                    for (Map.Entry<StitchKey, Object> me
                             : c.values().entrySet())
                        yy.print(" "+me.getKey()+"="
                                 +Util.toString(me.getValue(), 0));
                    yy.println("\n");
                    ++m;
                }
            }
            zz.close();
            yy.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * CliqueVisitor interface
     */
    public boolean clique (Clique clique) {
        //logger.info("Processing clique "+clique+"...");
        int index = cliques.size();

        Set<DataSource> sources = new HashSet<DataSource>();
        for (Entity e : clique) {
            sources.add(e.datasource());
        }
        
        Map<StitchKey, Object> values = clique.values();
        /*
        for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
            switch (me.getKey()) {
            case H_LyChI_L5:
            case H_LyChI_L4:
                if (DEBUG) {
                    logger.info("Transitive closure on clique "
                                +me.getKey()+"="+me.getValue()+"...");
                }
                closure (clique, me.getKey());
                break;

            default:
                int count = ef.getStitchedValueCount
                    (me.getKey(), me.getValue());
                // conservative.. maximum span of datasource on this clique
                int size = clique.size()*(clique.size()-1)/2;
                if (size == count && clique.size() == sources.size()) {
                    if (DEBUG) {
                        logger.info("Transitive closure on clique "
                                    +me.getKey()+"="+me.getValue()+"...");
                    }
                    closure (clique, me.getKey());
                }
            }
        }
        */

        // map entity to its cliques
        for (Entity e : clique) {
            Set<Clique> cliques = entities.get(e);
            if (cliques == null) {
                entities.put(e, cliques = new HashSet<Clique>());
            }
            cliques.add(clique);
        }
        
        cliques.add(clique);
        
        return true;
    }

    void closure (Clique clique, StitchKey... keys) {
        Entity[] entities = clique.entities();
        Map<StitchKey, Object> values = clique.values();
        for (StitchKey key : keys) {
            Object value = values.get(key);
            if (value != null)
                closure (key, value, entities);
        }
    }
    
    void closure (StitchKey key, Object value, Entity... entities) {
        Expander ex = new Expander (key, value);
        for (Entity e : entities) {
            if (!eqv.contains(e.getId())) {
                //System.out.println("** New path: "+key+"="+value);
                ex.start = e;
                e.walk(ex);
            }
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            System.err.println("Usage: "+CliqueEntityResolution.class.getName()
                               +" DB [LABELS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            CliqueEntityResolution cer = new CliqueEntityResolution ();
            AtomicInteger total = new AtomicInteger ();
            cer.resolve(ef, comp -> {
                    //System.out.println(c.length+" component resolved");
                    System.out.println("+++++ "+total+" size="
                                       +comp.size()+" score="
                                       +String.format("%1$.3f", comp.score())
                                       +" +++++");
                    for (Entity e : comp) {
                        Map<StitchKey, Object> keys = e.keys();
                        System.out.print
                            (String.format
                             ("%1$10s[%2$ 10d]",
                              e.payloadId() != null
                              ? e.payloadId().toString() : "", e.getId())
                             +": "+keys.size()+"={");
                        int i = 0;
                        for (Map.Entry<StitchKey, Object> me
                                 : keys.entrySet()) {
                            System.out.print(me.getKey()+"="
                                             +Util.toString(me.getValue()));
                            if (++i < keys.size())
                                System.out.print(" ");
                        }
                        System.out.println("}");
                    }
                    System.out.println();
                    total.incrementAndGet();
                });
            System.out.println(total+" total components!");
        }
        finally {
            graphDb.shutdown();
        }
    }
}
