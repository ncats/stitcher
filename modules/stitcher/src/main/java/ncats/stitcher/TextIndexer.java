package ncats.stitcher;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.facet.*;
import org.apache.lucene.facet.taxonomy.directory.*;
import org.apache.lucene.facet.taxonomy.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;

public class TextIndexer extends TransactionEventHandler.Adapter
    implements AutoCloseable {
    static final Logger logger = Logger.getLogger(TextIndexer.class.getName());

    static final Label DATA = Label.label("DATA");
    public static final String FIELD_TEXT = "text";
    public static final String FIELD_ID = "@id";
    public static final String FIELD_PROPS = "@props";

    final protected GraphDatabaseService gdb;
    final protected File dbdir;
    final protected FieldType tvFieldType;
    final protected Directory indexDir;
    final protected IndexWriter indexWriter;
    final protected Directory taxonDir;
    final protected DirectoryTaxonomyWriter taxonWriter;
    final protected FacetsConfig facetConfig;
    final protected SearcherManager searcherManager;

    protected TextIndexer (GraphDatabaseService gdb, File dir)
        throws Exception {
        File text = new File (dir, "text");
        text.mkdirs();
        indexDir = new NIOFSDirectory (text.toPath());
        indexWriter = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        File taxon = new File (dir, "taxon");
        taxon.mkdirs();
        taxonDir = new NIOFSDirectory (taxon.toPath());
        taxonWriter = new DirectoryTaxonomyWriter (taxonDir);
        facetConfig = configFacets ();

        tvFieldType = new FieldType (TextField.TYPE_STORED);
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();

        searcherManager = new SearcherManager
            (indexWriter, new SearcherFactory ());
        
        gdb.registerTransactionEventHandler(this);
        this.dbdir = dir;
        this.gdb = gdb;
    }

    public void close () throws Exception {
        searcherManager.close();
        IOUtils.close(indexWriter, indexDir, taxonWriter, taxonDir);
        gdb.unregisterTransactionEventHandler(this);
    }

    /*
     * should be overriden by subclasses
     */
    protected FacetsConfig configFacets () {
        return new FacetsConfig ();
    }

    Document index (Document doc, String field, String s) {
        doc.add(new TextField (field, s, Field.Store.NO));
        int pos = s.indexOf(':');
        if (pos > 0) {
            doc.add(new TextField (s.substring(0, pos),
                                   s.substring(pos+1),
                                   Field.Store.NO));
            doc.add(new TextField (FIELD_TEXT, s.substring(pos+1),
                                   Field.Store.NO));
        }
        else 
            doc.add(new TextField (FIELD_TEXT, s, Field.Store.NO));
        return doc;
    }
    
    protected Document instrument (String type, org.neo4j.graphdb.Entity e) {
        Document doc = new Document ();
        doc.add(new StringField
                (FIELD_ID, type+":"+e.getId(), Field.Store.YES));
        for (Map.Entry<String, Object> me
                 : e.getAllProperties().entrySet()) {
            Object value = me.getValue();
            if (value instanceof String[]) {
                String[] vals = (String[])value;
                for (String s : vals) {
                    index (doc, me.getKey(), s);
                }
            }
            else if (value instanceof String) {
                index (doc, me.getKey(), (String)value);
            }
        }
        return doc;
    }

    @Override
    public Object beforeCommit (TransactionData data) throws Exception {
        List<Document> docs = new ArrayList<>();
        for (Node node : data.createdNodes()) {
            //logger.info("new node "+node.getId()+" "+node.getLabels());
            if (node.hasLabel(DATA)) {
                docs.add(instrument (DATA.name(), node));
            }
        }
        return docs;
    }

    @Override
    public void afterCommit (TransactionData data, Object state) {
        List<Document> docs = (List<Document>)state;
        try {
            for (Document d : docs) {
                indexWriter.addDocument(d);
                //logger.info("adding doc "+d);
            }
            indexWriter.flush();
        }
        catch (IOException ex) {
            logger.log(Level.SEVERE, "Can't index document", ex);
        }
    }

    @Override
    public void afterRollback(TransactionData data, Object state) {
    }

    public List<Node> search (String query, int max) throws Exception {
        return search (query, 0, max);
    }
    
    public List<Node> search (String query, int skip, int top)
        throws Exception {
        QueryParser parser = new QueryParser
            (FIELD_TEXT, indexWriter.getAnalyzer());
        return search (parser.parse(query), skip, top);
    }
    
    public List<Node> search (Query query, int skip, int top)
        throws Exception {
        List<Node> results = new ArrayList<>();
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            TopDocs hits = searcher.search(query, top+skip);
            int size = Math.min(skip+top, hits.totalHits);
            for (int i = skip; i < size; ++i) {
                Document doc = searcher.doc(hits.scoreDocs[i].doc);
                String did = doc.get(FIELD_ID);
                if (did != null) {
                    int pos = did.indexOf(':');
                    if (pos > 0) {
                        try {
                            long id = Long.parseLong(did.substring(pos+1));
                            Node n = gdb.getNodeById(id);
                            if (n != null && n.hasLabel(DATA))
                                results.add(n);
                        }
                        catch (NumberFormatException ex) {
                            logger.log(Level.SEVERE, "Bogus id field: "+did,ex);
                        }
                    }
                }
            }
        }
        return results;
    }
}
