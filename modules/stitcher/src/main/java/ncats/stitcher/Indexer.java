package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import java.util.logging.Logger;
import java.util.logging.Level;

import org.apache.lucene.store.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.*;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.*;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;


public class Indexer {
    static final Logger logger = Logger.getLogger(Indexer.class.getName());
    
    static final String DEFAULT_FACET = "facet"; // facet index
    static final String DEFAULT_TEXT = "text"; // lucene index
    static final String DEFAULT_SUGGEST = "suggest"; // suggest index
    
    
    final ReentrantLock lock = new ReentrantLock ();
    protected File base;
    protected Directory textDir;
    protected Directory facetsDir;
    protected IndexWriter indexWriter;
    protected DirectoryTaxonomyWriter facetsWriter;
    protected FacetsConfig facetsConfig; 
    protected IndexSearcher indexSearcher;

    static final Map<File, Indexer> INSTANCES = new ConcurrentHashMap<>();

    protected Indexer (File base) throws IOException {
        if (!base.exists())
            base.mkdirs();
        
        File dir = new File (base, DEFAULT_TEXT);
        dir.mkdirs();
        textDir = new NIOFSDirectory (dir.toPath());
        IndexWriterConfig config =
            new IndexWriterConfig (new StandardAnalyzer ());
        indexWriter = new IndexWriter (textDir, config);
        
        dir = new File (base, DEFAULT_FACET);
        dir.mkdirs();
        facetsDir = new NIOFSDirectory (dir.toPath());
        facetsWriter = new DirectoryTaxonomyWriter (facetsDir);
        facetsConfig = new FacetsConfig ();

        this.base = base;
        INSTANCES.put(base, this);
    }

    public void shutdown () {
        lock.lock();
        try {
            if (INSTANCES.containsKey(base)) {
                IOUtils.close(indexWriter);
                IOUtils.close(textDir);
                IOUtils.close(facetsWriter);
                IOUtils.close(facetsDir);
                INSTANCES.remove(base);
            }
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, "Can't close Lucene handles", ex);
        }
        finally {
            lock.unlock();
        }
    }

    public synchronized static Indexer getInstance (File base)
        throws IOException {
        Indexer indexer = INSTANCES.get(base);
        if (indexer == null) {
            indexer = new Indexer (base);
        }
        return indexer;
    }

    public void add (Entity ent) {
        String FIELD_ID = ent.getClass().getName()+".__id";
    }

    public void addIfAbsent (Entity ent) {
        String FIELD_ID = ent.getClass().getName()+".__id";
    }

    public void update (Entity ent) {
    }

    public void remove (Entity ent) {
    }
}
