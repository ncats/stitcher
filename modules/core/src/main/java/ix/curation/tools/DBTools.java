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
import ix.curation.Util;

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
        Util.dump(comp);
        comp.cliques(c -> {
                Util.dump (c);
                return true;
            }, StitchKey.H_LyChI_L4, StitchKey.H_LyChI_L5,
            StitchKey.I_CAS, StitchKey.I_UNII);
    }

    public void cliques () {
        ef.cliqueEnumeration(clique -> {
                Util.dump(clique);
                return true;
            });
    }

    public void cliques (StitchKey key, Object value) {
        System.out.println("+++++++ Cliques for "+key+"="+value+" +++++++");
        ef.cliqueEnumeration(key, value, clique -> {
                Util.dump(clique);
                return true;
            });
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
            else
                System.err.println("Unknown command: "+cmd);
        }
        finally {
            graphDb.shutdown();
        }
    }
}
