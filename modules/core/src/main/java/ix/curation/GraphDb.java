package ix.curation;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;

import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.event.*;
import org.neo4j.tooling.GlobalGraphOperations;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.facet.index.CategoryDocumentBuilder;
import org.apache.lucene.util.Version;
import org.apache.lucene.analysis.KeywordAnalyzer;
import org.apache.lucene.facet.index.CategoryContainer;

/**
 * wrapper around GraphDatabaseService instance
 */
public class GraphDb extends TransactionEventHandler.Adapter {
    static final String DEFAULT_CACHE = "cache"; // cache
    static final String DEFAULT_TAXON = "taxon"; // facet index
    static final String DEFAULT_INDEX = "index"; // lucene index
    static final String DEFAULT_SUGGEST = "suggest"; // suggest index
    static final String DEFAULT_IXDB = "ix.db"; // top-level

    static final Logger logger = Logger.getLogger(GraphDb.class.getName());

    static final Map<File, GraphDb> INSTANCES =
        new ConcurrentHashMap<File, GraphDb>();

    protected final File dir;
    protected final GraphDatabaseService gdb;
    protected final AtomicLong refs = new AtomicLong (1l);
    protected final CacheFactory cache;
    protected final AtomicLong lastUpdated = new AtomicLong ();

    protected IndexWriter indexWriter;
    protected DirectoryTaxonomyWriter taxonWriter;
    protected CategoryDocumentBuilder taxonBuilder;

    protected GraphDb (File dir) throws IOException {
        this (dir, null);
    }

    static IndexWriterConfig createLuceneConfig () {
        IndexWriterConfig conf = new IndexWriterConfig
            (Version.LUCENE_CURRENT, new KeywordAnalyzer ());
        return conf;
    }
    
    protected GraphDb (File dir, CacheFactory cache) throws IOException {
        gdb = new GraphDatabaseFactory().newEmbeddedDatabase(dir);
        gdb.registerTransactionEventHandler(this);

        File ixdb = new File (dir, DEFAULT_IXDB);
        ixdb.mkdirs();

        File path = new File (ixdb, DEFAULT_INDEX);
        path.mkdirs();
        Directory luceneDir = new NIOFSDirectory (path);
        indexWriter = new IndexWriter (luceneDir, createLuceneConfig ());

        path = new File (ixdb, DEFAULT_TAXON);
        path.mkdirs();
        luceneDir = new NIOFSDirectory (path);
        taxonWriter = new DirectoryTaxonomyWriter
            (luceneDir, IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        taxonBuilder = new CategoryDocumentBuilder (taxonWriter);
        
        // this must be initialized after graph initialization
        if (cache == null) {
            this.cache = CacheFactory.getInstance
                   (new File (ixdb, DEFAULT_CACHE));
        }
        else {
            this.cache = cache;
        }
        this.dir = dir;
    }

    static Term createTerm (Node node) {
        Term t = new Term (node.getClass().getName(),
                           String.valueOf(node.getId()));
        return t;
    }

    static Document createDoc (Node node) {
        Document doc = new Document ();
        doc.add(new Field (node.getClass().getName(),
                           String.valueOf(node.getId()), Field.Store.YES,
                           Field.Index.NOT_ANALYZED_NO_NORMS));
        return doc;
    }

    void updateIndex (TransactionData data) throws Exception {
        for (Node node : data.deletedNodes()) {
            indexWriter.deleteDocuments(createTerm (node));
        }

        for (Node node : data.createdNodes()) {
            Document doc = createDoc (node);
            CategoryContainer cats = new CategoryContainer ();
            for (Label label : node.getLabels()) {
                cats.addCategory(new CategoryPath ("node", label.name()));
            }
            taxonBuilder.setCategories(cats);
            indexWriter.addDocument(taxonBuilder.build(doc));
        }

        indexWriter.commit();
    }

    @Override
    public void afterCommit (TransactionData data, Object state) {
        lastUpdated.set(System.currentTimeMillis());
        try (Transaction tx = gdb.beginTx()) {
            updateIndex (data);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public long getLastUpdated () { return lastUpdated.get(); }
    public GraphDatabaseService graphDb () { return gdb; }
    public CacheFactory getCache () { return cache; }
    public File getPath () { return dir; }
    public void shutdown () {
        if (refs.decrementAndGet() <= 1l) {
            INSTANCES.remove(dir);
            cache.shutdown();
            gdb.shutdown();
        }
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
        }
        return null;
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

    public static synchronized GraphDb getInstance
        (File dir) throws IOException {
        return getInstance (dir, null);
    }
    
    public static synchronized GraphDb getInstance
        (File dir, CacheFactory cache) throws IOException {
        GraphDb gdb = INSTANCES.get(dir);
        if (gdb == null) {
            INSTANCES.put(dir, gdb = new GraphDb (dir, cache));
        }
        else {
            gdb.refs.incrementAndGet();
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
                        gdb.cache.shutdown();
                        logger.info
                            ("##### Shutting Down Graph Database: "
                             +gdb.dir+" ("+gdb.refs+") #####");
                    }
                }
            });
    }
}
