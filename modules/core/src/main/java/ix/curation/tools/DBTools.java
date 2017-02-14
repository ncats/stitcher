package ix.curation.tools;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.atomic.AtomicInteger;

import ix.curation.EntityFactory;
import ix.curation.Entity;
import ix.curation.DataSource;
import ix.curation.DataSourceFactory;
import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.AuxNodeType;
import ix.curation.CliqueVisitor;
import ix.curation.Clique;
import ix.curation.Props;
import ix.curation.Component;

public class DBTools {
    static final Logger logger = Logger.getLogger(DBTools.class.getName());

    final EntityFactory ef;
    final DataSourceFactory dsf;
    
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
            System.err.println("Datasource "+source+" not available!");
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
        System.out.println
            ("+++++++ Cliques for component "+comp.getId()
             +" ("+comp.size()+") +++++++");
        System.out.println("nodes: "+comp.nodeSet());
        
        comp.cliques(c -> {
                    dumpClique (c);
                    return true;
            });
    }

    public void cliques () {
        ef.cliqueEnumeration(new CliqueVisitor () {
                public boolean clique (Clique clique) {
                    dumpClique (clique);
                    return true;
                }
            });
    }

    void dumpClique (Clique clique) {
        System.out.println
            ("+++++++ Clique "+clique.getId()+" +++++++");
        System.out.println("size: "+clique.size());
        System.out.println(String.format("score: %1$.3f",
                                         clique.score()));
        if (clique.size() > 0) {
            Entity e = clique.entities()[0].parent();
            System.out.println("parent: " +e.getId()
                               +" ("+e.get(Props.RANK)+")");
            System.out.print("nodes:");
            for (Entity n : clique)
                System.out.print(" "+n.getId());
            System.out.println();
        }

        System.out.println("-- stitch keys --");
        for (Map.Entry<StitchKey, Object> me
                 : clique.values().entrySet()) {
            System.out.print(me.getKey()+":");
            Object val = me.getValue();
            if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    System.out.print(" "+Array.get(val, i));
                }
            }
            else
                System.out.print(" "+val);
            System.out.println();
        }
        System.out.println();
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
            else
                System.err.println("Unknown command: "+cmd);
        }
        finally {
            graphDb.shutdown();
        }
    }
}
