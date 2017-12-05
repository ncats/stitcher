package ncats.stitcher.tools;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import ncats.stitcher.*;

public class DBTools {
    static final Logger logger = Logger.getLogger(DBTools.class.getName());

    final EntityFactory ef;
    final DataSourceFactory dsf;

    static class LabelMapper implements Function<String, String> {
        Map<String, String> mapper = new TreeMap<>();
        final String prefix;

        protected LabelMapper (String prefix) {
            if (prefix == null)
                throw new IllegalArgumentException ("Prefix can't be null");
            this.prefix = prefix;
        }

        public String apply (String label) {
            String m = mapper.get(label);
            if (m == null) {
                m = String.format(prefix+"%1$03d", mapper.size()+1);
                mapper.put(label, m);
            }
            return m;
        }

        public void dumpkeys (OutputStream os) {
            PrintStream ps = new PrintStream (os);
            TreeSet<Map.Entry<String, String>> keys = new TreeSet<>((a,b) -> {
                    return a.getValue().compareTo(b.getValue());
                });
            keys.addAll(mapper.entrySet());
            for (Map.Entry<String, String> me : keys)
                ps.println(me.getValue()+"\t"+me.getKey());
        }
    }

    static class EdgeLabelMapper extends LabelMapper {
        Random rand = new Random (12345l);
        public EdgeLabelMapper () {
            super ("E");
        }
        /*
        @Override
        public String apply (String label) {
            String map = super.apply(label);
            if (Math.abs(rand.nextGaussian()) > .9)
                map = null;
            return map;
        }
        */
    }

    static class VertexLabelMapper extends LabelMapper {
        public VertexLabelMapper () {
            super ("V");
        }
    }

    static class CountOperator implements EntityVisitor {
        final public Map<Object, Integer> counts = new HashMap<>();
        final public Map<StitchKey, Integer> keys =
            new EnumMap<>(StitchKey.class);

        public boolean visit (Entity.Traversal traversal,
                              Entity.Triple triple) {
            Map<StitchKey, Object> values = triple.values();
            for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
                Object val = me.getValue();
                if (val.getClass().isArray()) {
                    int len = Array.getLength(val);
                    for (int i = 0; i < len; ++i) {
                        Object v = Array.get(val, i);
                        Integer c = counts.get(v);
                        counts.put(v, c== null ? 1 : (c+1));
                    }
                }
                else {
                    Integer c = counts.get(val);
                    counts.put(val, c==null ? 1 : (c+1));
                }
                
                Integer c = keys.get(me.getKey());
                keys.put(me.getKey(), c==null?1 : (c+1));
            }
            return true;
        }
    }

    static class ScoreOperator implements EntityVisitor {
        public final Map<Integer, Integer> scores = new TreeMap<>();
        
        ScoreOperator () {
        }

        public boolean visit (Entity.Traversal traversal,
                              Entity.Triple triple) {
            Map<StitchKey, Object> values = triple.values();
            int score = 0;
            for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
                Object val = me.getValue();
                if (val.getClass().isArray()) {
                    int len = Array.getLength(val);
                    score += me.getKey().priority*len;
                }
                else {
                    score += me.getKey().priority;
                }
            }

            Integer c = scores.get(score);
            scores.put(score, c==null? 1 : (c+1));

            System.out.println(triple.source()+" "+triple.target()+" "+score);
            
            return true;
        }
    }
    
    public DBTools (GraphDb graphDb) {
        ef = new EntityFactory (graphDb);
        dsf = new DataSourceFactory (graphDb);
    }

    public void list () {
        Set<DataSource> sources = dsf.datasources();
        System.out.println("++++++ "+sources.size()+" datasources +++++++++");
        for (DataSource source : sources) {
            System.out.println("Name: "+source.getName());
            System.out.println(" Key: "+source.getKey());
            if (source.toURI() != null) {
                System.out.println(" URI: "+source.toURI());
            }
            System.out.println("Size: "+source.get(DataSource.INSTANCES));
            System.out.println();
        }
    }

    public void path (long start, long end, StitchKey... keys) {
        Entity e0 = ef.entity(start);
        Entity e1 = ef.entity(end);
        if (keys == null || keys.length == 0)
            keys = Entity.KEYS;
        Entity[] path = e0.anyPath(e1, keys);
        System.out.println("Any path between ("+start+") and ("+end+")...");
        if (path != null && path.length > 0) {
            for (int i = 1; i < path.length; ++i) {
                System.out.println(path[i-1].getId());
                Map<StitchKey, Object> stitches = path[i-1].keys(path[i]);
                System.out.println(".."+stitches);
            }
            System.out.println(e1.getId());
        }
    }

    public void traverse (Long... comps) {
        if (comps == null || comps.length == 0) {
            CountOperator oper = new CountOperator ();
            ef.traverse(oper);
            
            System.out.println("#### value histogram ####");
            Set<Object> sorted = new TreeSet<>((a,b) -> {
                    int d = oper.counts.get(b) - oper.counts.get(a);
                    if (d == 0)
                        d = a.toString().compareTo(b.toString());
                    return d;
                });
            sorted.addAll(oper.counts.keySet());
            
            for (Object key : sorted) {
                System.out.println("\""+key+"\" "+oper.counts.get(key));
            }
            
            System.out.println("#### key histogram ####");
            for (Map.Entry<StitchKey, Integer> me : oper.keys.entrySet())
                System.out.println(me.getKey()+" "+me.getValue());
        }
        else {
            for (Long c : comps) {
                Entity e = ef.entity(c);
                System.out.println("################## COMPONENT "+e.getId()
                                   +" #####################");
                CountOperator oper = new CountOperator ();
                e.traverse(oper);
                
                System.out.println("#### value histogram ####");
                for (Map.Entry<Object, Integer> me : oper.counts.entrySet())
                    System.out.println("\""+me.getKey()+"\" "+me.getValue());
                
                System.out.println("#### key histogram ####");
                for (Map.Entry<StitchKey, Integer> me : oper.keys.entrySet())
                    System.out.println(me.getKey()+" "+me.getValue());
            }
        }
    }
    
    public void delete (String source) {
        DataSource ds = dsf.getDataSourceByKey(source);
        if (ds != null) {
            System.out.println("## deleting source "+ds.getName()+"...");
            ef.delete(ds);
            ds.delete();
        }
        else {
            ef.delete(source);
            //System.err.println("Datasource "+source+" not available!");
        }
    }

    public void dump (OutputStream os, String source) throws IOException {
        PrintStream ps = new PrintStream (os);
        for (Iterator<Entity> it = ef.entities(source); it.hasNext(); ) {
            Entity e = it.next();
            ps.print(e.getId());
            ps.print("\t"+e.payloadId());
            ps.println();
        }
    }

    public void components () {
        components (100);
    }

    public void components (int N) {
        AtomicInteger cnt = new AtomicInteger ();
        Queue<Map.Entry<Long, Integer>> topN = new PriorityQueue<>
            ((a, b) -> {
                int d = b.getValue() - a.getValue();
                if (d == 0) {
                    if (a.getKey() < b.getKey()) d = -1;
                    else if (a.getKey() > b.getKey()) d = 1;
                }
                return d;
            });
        ef.maps(e -> {
                Map.Entry<Long, Integer> me = new AbstractMap.SimpleImmutableEntry
                    (e.getId(), (Integer)e.get(Props.RANK));
                System.out.println(me.getKey()+" "+me.getValue());
                topN.add(me);
                cnt.incrementAndGet();
            }, "COMPONENT");
        System.out.println("## "+cnt.get()+" component(s)!");
        System.out.println("## Top "+N+" components");
        for (int i = 0; i < Math.min(cnt.get(), N); ++i) {
            Map.Entry<Long, Integer> me = topN.poll();
            System.out.println(me.getKey()+" "+me.getValue());
        }
    }

    public void clique (long id) {
        Component comp = ef.component(id);
        Util.dump(comp);
        comp.cliques(c -> {
                Util.dump (c);
                return true;
            }, StitchKey.H_LyChI_L4, StitchKey.H_LyChI_L5,
            StitchKey.I_CAS, StitchKey.I_UNII);
    }

    public void cliques () {
        ef.cliques(clique -> {
                Util.dump(clique);
                return true;
            });
    }

    public void cliques (StitchKey key, Object value) {
        System.out.println("+++++++ Cliques for "+key+"="+value+" +++++++");
        ef.cliques(clique -> {
                Util.dump(clique);
                return true;
            }, key, value);
    }

    public void find (String prop, Object value) {
        System.out.println("+++++++++ Entities with "+prop+"=\""
                           +value+"\" ++++++++");
        int count = 0;
        for (Iterator<Entity> it = ef.find(prop, value);
             it.hasNext(); ++count) {
            Entity e = it.next();
            System.out.println(e.getId()+": "+e.properties());
        }
        System.out.println("### "+count+" entities found!");
    }

    public void stats (long id) throws IOException {
        Component comp = ef.component(id);
        Set<Clique> cliques = new TreeSet<>((a,b) -> {
                int d = b.size() - a.size();
                for (int i = 0; i < a.size() && d == 0; ++i) {
                    if (a.nodes()[i] < b.nodes()[i]) d = -1;
                    else if (a.nodes()[i] > b.nodes()[i]) d = 1;
                }
                return d;
            });
        logger.info("## dumping component "+id+"...");
        PrintStream ps = new PrintStream (new FileOutputStream ("comp_"+id+".txt"));
        for (StitchKey key : EnumSet.allOf(StitchKey.class)) {
            Map<Object, Integer> stats = comp.stats(key);
            if (stats.size() > 0) {
                ps.println("\n[***************** "+key+" *****************]");
                Set sorted = new TreeSet ((a,b) -> {
                        int d = stats.get(b) - stats.get(a);
                        if (d == 0)
                            d = ((Comparable)a).compareTo((Comparable)b);
                        return d;
                    });
                sorted.addAll(stats.keySet());
                for (Object k : sorted) {
                    Integer count = stats.get(k);
                    ps.println("## "+k+": "+count);
                    if (count > 1) {
                        ef.cliques(clique -> {
                                Util.dump(ps, clique);
                                cliques.add(clique);
                                return true;
                            }, key, k);
                    }
                }
            }
        }

        ps.println("\n[**************** CLIQUES ******************]");
        for (Clique c : cliques)
            Util.dump(ps, c);

        ps.close();
    }

    public void export (String name, List<Long> comps) throws IOException {
        logger.info("Exporting graphs to "+name+"...");
        VertexLabelMapper vm = new VertexLabelMapper ();
        EdgeLabelMapper em = new EdgeLabelMapper ();
        FileOutputStream fos = new FileOutputStream (name+".txt");
        ef.export(fos, vm, em, comps.toArray(new Long[0]));
        fos.close();
        fos = new FileOutputStream (name+".vkeys");
        vm.dumpkeys(fos);
        fos.close();
        fos = new FileOutputStream (name+".ekeys");
        em.dumpkeys(fos);
        fos.close();
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+DBTools.class.getName()
                               +" DB Command [ARGS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            DBTools dbt = new DBTools (graphDb);
            String cmd = argv[1];
            if ("list".equalsIgnoreCase(cmd)
                || "datasources".equalsIgnoreCase(cmd)) {
                dbt.list();
            }
            else if ("delete".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    dbt.delete(argv[2]);
                }
                else {
                    System.err.println("No datasource specified!");
                }
            }
            else if ("components".equalsIgnoreCase(cmd)) {
                dbt.components();
            }
            else if ("cliques".equalsIgnoreCase(cmd)) {
                if (argv.length > 3) {
                    StitchKey key = StitchKey.valueOf(argv[2]);
                    dbt.cliques(key, argv[3]);
                }
                else
                    dbt.cliques();
            }
            else if ("traverse".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    List<Long> comps = new ArrayList<>();
                    for (int i = 2; i < argv.length; ++i)
                        comps.add(Long.parseLong(argv[i]));
                    dbt.traverse(comps.toArray(new Long[0]));
                }
                else
                    dbt.traverse();
            }
            else if ("clique".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    dbt.clique(Long.parseLong(argv[2]));
                }
                else {
                    System.err.println("No component id specified!");
                }
            }
            else if ("path".equalsIgnoreCase(cmd)) {
                if (argv.length > 3) {
                    Set<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
                    for (int i = 4; i < argv.length; ++i) {
                        keys.add(StitchKey.valueOf(argv[i]));
                    }
                    dbt.path(Long.parseLong(argv[2]), Long.parseLong(argv[3]),
                             keys.toArray(new StitchKey[0]));
                }
                else {
                    System.err.println("START END [KEYS...]");
                }
            }
            else if ("dump".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    dbt.dump(System.out, argv[2]);
                }
                else {
                    System.err.println("No datasource specified!");
                }
            }
            else if ("find".equalsIgnoreCase(cmd)) {
                if (argv.length > 3) {
                    dbt.find(argv[2], argv[3]);
                }
                else {
                    System.err.println("No property and value specified!");
                }
            }
            else if ("stats".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    for (int i = 2; i < argv.length; ++i) {
                        dbt.stats(Long.parseLong(argv[i]));
                    }
                }
                else {
                    System.err.println("No component specified!");
                }
            }
            else if ("export".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    List<Long> comps = new ArrayList<>();
                    for (int i = 2; i < argv.length; ++i)
                        comps.add(Long.parseLong(argv[i]));
                    System.out.println("loading "+comps.size()+" components for export...");
                    dbt.export("graph_"+argv[2], comps);
                }
                else {
                    System.err.println("No component specified!");
                }
            }
            else
                System.err.println("Unknown command: "+cmd);
        }
        finally {
            graphDb.shutdown();
        }
    }
}
