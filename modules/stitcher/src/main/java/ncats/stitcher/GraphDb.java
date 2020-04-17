package ncats.stitcher;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import java.util.function.Function;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.lambdas.Uncheck;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.IndexCreator;


/**
 * wrapper around GraphDatabaseService instance
 */
public class GraphDb extends TransactionEventHandler.Adapter
    implements KernelEventHandler {
    
    static final String DEFAULT_CACHE = "cache"; // cache

    static final Logger logger = Logger.getLogger(GraphDb.class.getName());

    static final Map<File, GraphDb> INSTANCES =
        new ConcurrentHashMap<File, GraphDb>();

    protected final File dir;
    protected final GraphDatabaseService gdb;
    protected CacheFactory cache;
    protected boolean localCache;
    protected final AtomicLong lastUpdated = new AtomicLong ();
    
    protected final File indexDir;
    protected final Map<File, TextIndexer> indexers
        = new ConcurrentHashMap<>();
    
    protected GraphDb (File dir) throws IOException {
        this (dir, null);
    }

    public void createIndex (Label label, String name) {
        createIndex(label, name, 0);
    }
    
    public void createIndex (Label label, String name, int wait) {
        try (Transaction tx = gdb.beginTx()) {
            IndexCreator index = gdb.schema().indexFor(label).on(name);
            IndexDefinition def = index.create();
            if (wait > 0) {
                gdb.schema().awaitIndexOnline
                    (def, wait, TimeUnit.SECONDS);
            }
            tx.success();
        }
        catch (Exception ex) {
            logger.info("Index \""+name
                        +"\" already exists for nodes "+label+"!");
        }
    }
    
    protected GraphDb (File dir, CacheFactory cache) throws IOException {
        GraphDatabaseSettings.BoltConnector bolt =
            GraphDatabaseSettings.boltConnector("0");
        gdb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(dir)
            .setConfig(GraphDatabaseSettings.dump_configuration, "true")
            /*
            .setConfig(bolt.type, "BOLT" )
            .setConfig(bolt.enabled, "true" )
            .setConfig(bolt.address, "0.0.0.0:7687" )
            .setConfig("dbms.connector.http.address", "0.0.0.0:7474")
            .setConfig("dbms.connector.http.enabled", "true")
            .setConfig("dbms.connector.http.type", "HTTP")
            */
            .newGraphDatabase();

        /*
        try (Transaction tx = gdb.beginTx()) {
            createIndex (CNode.CLASS_LABEL, Props.PARENT);
            createIndex (AuxNodeType.SNAPSHOT, Props.PARENT);
            tx.success();
        }
        */
        
        gdb.registerTransactionEventHandler(this);
        gdb.registerKernelEventHandler(this);

        indexDir = new File (dir, "index");
        if (!indexDir.exists()) {
            indexDir.mkdirs();
        }

        // this must be initialized after graph initialization
        if (cache == null) {
            this.cache = CacheFactory.getInstance
                   (new File (indexDir, DEFAULT_CACHE));
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
        for (TextIndexer indexer : indexers.values()) {
            try {
                indexer.close();
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't close TextIndexer", ex);
            }
        }
        gdb.unregisterTransactionEventHandler(this);
        gdb.shutdown();
        if (localCache)
            cache.shutdown();
    }

    public CNode getNode (long id) {
        CNode cnode = null;
        try (Transaction tx = gdb.beginTx()) {
            Node node = gdb.getNodeById(id);
            if (node != null) {
                for (EntityType t : EnumSet.allOf(EntityType.class)) {
                    if (node.hasLabel(t)) {
                        cnode = Entity._getEntity(node);
                        break;
                    }
                }
                if (cnode == null)
                    cnode = new CNode (node);
            }
            tx.success();
        }
        return cnode;
    }

    public TextIndexer getTextIndexer (String name) throws Exception {
        return getTextIndexer (name, true);
    }
    
    public TextIndexer getTextIndexer (String name, boolean createIfAbsent)
        throws Exception {
        File dir = new File (indexDir, name);
        TextIndexer indexer = indexers.get(dir);
        if (indexer == null) {
            if (!dir.exists() && !createIfAbsent)
                throw new IllegalArgumentException
                    ("TextIndexer \""+name+"\" doesn't exist!");
            indexer = new TextIndexer (gdb, dir);
            indexers.put(dir, indexer);
        }
        return indexer;
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
        return createTempDb ("_ix");
    }

    public static GraphDb createTempDb (String prefix) throws IOException {
        File file = Files.createTempDirectory(prefix).toFile();
        GraphDb graphDb = new GraphDb (file);
        INSTANCES.put(file, graphDb);
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
        return INSTANCES.computeIfAbsent
            (dir, Uncheck.throwingFunction(f-> new GraphDb(f, cache)));
    }

    public static GraphDb getInstance (GraphDatabaseService gdb) {
        for (GraphDb db : INSTANCES.values())
            if (db.graphDb() == gdb)
                return db;
        return null;
    }

    public Indexer getIndexer (Integer version) throws IOException {
        return Indexer.getInstance(new File (indexDir, "v"+version));
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
