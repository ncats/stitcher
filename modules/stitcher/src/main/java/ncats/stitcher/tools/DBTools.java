package ncats.stitcher.tools;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Entity;
import ncats.stitcher.DataSource;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.AuxNodeType;
import ncats.stitcher.CliqueVisitor;
import ncats.stitcher.Clique;
import ncats.stitcher.Props;
import ncats.stitcher.Component;
import ncats.stitcher.Util;

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
        AtomicInteger ai = new AtomicInteger (0);
        ef.components(c -> {
                int score = c.score().intValue();               
                if (score > 100) {
                    System.out.println("++++++++++++ Component "
                                       +ai.get()+" ++++++++++++");
                    System.out.println("score: "+score);
                    System.out.println("size: "+c.size());
                }
                ai.getAndIncrement();
            });
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
            else if ("clique".equalsIgnoreCase(cmd)) {
                if (argv.length > 2) {
                    dbt.clique(Long.parseLong(argv[2]));
                }
                else {
                    System.err.println("No component id specified!");
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
