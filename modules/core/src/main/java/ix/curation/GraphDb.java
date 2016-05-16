package ix.curation;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;

/**
 * wrapper around GraphDatabaseService instance
 */
public class GraphDb extends TransactionEventHandler.Adapter
    implements KernelEventHandler {
    
    static final String DEFAULT_CACHE = "cache"; // cache
    static final String DEFAULT_TAXON = "taxon"; // facet index
    static final String DEFAULT_INDEX = "index"; // lucene index
    static final String DEFAULT_SUGGEST = "suggest"; // suggest index
    static final String DEFAULT_IXDB = "ix.db"; // top-level

    static final Logger logger = Logger.getLogger(GraphDb.class.getName());

    /**
     * VERY IMPORTANT: by default version neo4j version 3 set this
     * to be true, which in turn uses the BlockTreeOrdsPostingsFormat
     * codec that generates a large number of files when traversing the 
     * nodes and as a result we get "Too many open files" exceptions.
     */
    static {
        System.setProperty("org.neo4j.kernel.api.impl.index.IndexWriterConfigs.block.tree.ords.posting.format", "false");
    }

    static final Map<File, GraphDb> INSTANCES =
        new ConcurrentHashMap<File, GraphDb>();

    protected final File dir;
    protected final GraphDatabaseService gdb;
    protected CacheFactory cache;
    protected boolean localCache;
    protected final AtomicLong lastUpdated = new AtomicLong ();

    protected GraphDb (File dir) throws IOException {
        this (dir, null);
    }

    protected GraphDb (File dir, CacheFactory cache) throws IOException {
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            .newGraphDatabase();

        try (Transaction tx = gdb.beginTx()) {
            IndexCreator index = gdb.schema().indexFor(CNode.CLASS_LABEL)
                .on(Props.PARENT);
            try {
                IndexDefinition def = index.create();
                /*
                  gdb.schema().awaitIndexOnline
                  (def, 100, TimeUnit.SECONDS);*/
            }
            catch (Exception ex) {
                logger.info("Index \""+Props.PARENT+"\" already exists!");
            }

            index = gdb.schema().indexFor(AuxNodeType.SNAPSHOT)
                .on(Props.PARENT);      
            try {
                IndexDefinition def = index.create();
            }
            catch (Exception ex) {
                logger.info("Index \""+AuxNodeType.SNAPSHOT
                            +"\" already exists!");
            }
    
            tx.success();
        }
        
        gdb.registerTransactionEventHandler(this);
        gdb.registerKernelEventHandler(this);

        // this must be initialized after graph initialization
        if (cache == null) {
            File ixdb = new File (dir, DEFAULT_IXDB);
            ixdb.mkdirs();
            
            this.cache = CacheFactory.getInstance
                   (new File (ixdb, DEFAULT_CACHE));
            localCache = true;
        }
        else {
            this.cache = cache;
            localCache = false;
        }
        this.dir = dir;
    }

    @Override
    public void afterCommit (TransactionData data, Object state) {
        lastUpdated.set(System.currentTimeMillis());
    }

    public long getLastUpdated () { return lastUpdated.get(); }
    public GraphDatabaseService graphDb () { return gdb; }
    public CacheFactory getCache () { return cache; }
    public void setCache (CacheFactory cache) {
        if (cache == null)
            throw new IllegalArgumentException ("Cache can't be null");
        this.cache.shutdown();
        this.cache = cache;
    }
    
    public File getPath () { return dir; }
    public void shutdown () {
        gdb.shutdown();
        if (localCache)
            cache.shutdown();
    }

    public CNode getNode (long id) {
        try (Transaction tx = gdb.beginTx()) {
            Node node = gdb.getNodeById(id);
            if (node != null) {
                for (EntityType t : EnumSet.allOf(EntityType.class)) {
                    if (node.hasLabel(t))
                        return Entity._getEntity(node);
                }
                return new CNode (node);
            }
            tx.success();
        }
        return null;
    }

    /*
     * KernelEventHandler
     */
    public void beforeShutdown () {
        logger.info("Instance "+dir+" shutting down...");
        INSTANCES.remove(dir);
    }

    public Object getResource () {
        return null;
    }

    public void kernelPanic (ErrorState error) {
        logger.log(Level.SEVERE, "Graph kernel panic: "+error);
    }

    public KernelEventHandler.ExecutionOrder orderComparedTo
        (KernelEventHandler other) {
        return KernelEventHandler.ExecutionOrder.DOESNT_MATTER;
    }

    public static GraphDb createTempDb () throws IOException {
        return createTempDb (null, null);
    }

    public static GraphDb createTempDb (String name) throws IOException {
        return createTempDb (name, null);
    }

    public static GraphDb createTempDb (File temp) throws IOException {
        return createTempDb (null, temp);
    }
    
    public static GraphDb createTempDb (String name, File temp)
        throws IOException {
        return createTempDb ("_ix"+(name != null ? name:""), ".db", temp);
    }
    
    public static GraphDb createTempDb (String prefix,
                                        String suffix, File temp)
        throws IOException {
        File junk = File.createTempFile(prefix, suffix, temp);
        File parent = temp == null ? junk.getParentFile() : temp;
        junk.delete();
        junk = new File (parent, junk.getName());
        junk.mkdirs();
        GraphDb graphDb = new GraphDb (junk);
        INSTANCES.put(junk, graphDb);
        return graphDb;
    }

    public static GraphDb getInstance (String dir) throws IOException {
        return getInstance (new File (dir));
    }
    
    public static GraphDb getInstance (String dir, CacheFactory cache)
        throws IOException {
        return getInstance (new File (dir), cache);
    }

    public static synchronized GraphDb getInstance (File dir)
        throws IOException {
        return getInstance (dir, null);
    }
    
    public static synchronized GraphDb getInstance
        (File dir, CacheFactory cache) throws IOException {
        GraphDb gdb = INSTANCES.get(dir);
        if (gdb == null) {
            INSTANCES.put(dir, gdb = new GraphDb (dir, cache));
        }
        return gdb;
    }

    public static GraphDb getInstance (GraphDatabaseService gdb) {
        for (GraphDb db : INSTANCES.values())
            if (db.graphDb() == gdb)
                return db;
        return null;
    }

    public static void addShutdownHook () {
        Runtime.getRuntime().addShutdownHook(new Thread() {
                // do shutdown work here
                public void run () {
                    for (GraphDb gdb : INSTANCES.values()) {
                        gdb.graphDb().shutdown();
                        logger.info
                            ("##### Shutting Down Graph Database: "
                             +gdb.dir+" #####");
                    }
                }
            });
    }
}
