package ix.curation.tools;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ix.curation.EntityFactory;
import ix.curation.Entity;
import ix.curation.DataSource;
import ix.curation.DataSourceFactory;
import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.AuxNodeType;

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
