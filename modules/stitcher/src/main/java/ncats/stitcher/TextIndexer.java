package ncats.stitcher;

import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.regex.*;
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
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;


public class TextIndexer extends TransactionEventHandler.Adapter
    implements AutoCloseable {
    static final Logger logger = Logger.getLogger(TextIndexer.class.getName());
    
    static final Pattern SLOP = Pattern.compile("~([\\d+])$");
    static final Pattern QUOTE =
        Pattern.compile("([\\+~-])?\"([^\"]+)\"(~[\\d+])?");
    static Pattern TOKEN = Pattern.compile("\\s*([^\\s]+|$)\\s*");

    public static class QueryTokenizer {
        public final List<String> tokens = new ArrayList<>();
        public QueryTokenizer (String text) {
            List<int[]> matches = new ArrayList<>();
            Matcher m = QUOTE.matcher(text);
            while (m.find()) {
                int[] r = new int[]{m.start(), m.end()};
                matches.add(r);
            }
            
            m = TOKEN.matcher(text);
            while (m.find()) {
                int s = m.start();
                int e = m.end();
                boolean valid = true;
                for (int[] r : matches) {
                    if ((s >= r[0] && s < r[1])
                        || (e > r[0] && e < r[1])) {
                        valid = false;
                        break;
                    }
                }
                if (valid && s < e)
                    matches.add(new int[]{s,e});
            }

            Collections.sort(matches, (a, b) -> a[0] - b[0]);
            for (int[] r : matches) {
                tokens.add(text.substring(r[0], r[1]).trim());
            }
        }
        
        public List<String> tokens () { return tokens; }
    }

    public static class Result {
        public final Node node;
        public final float score;

        Result (Node node, float score) {
            this.node = node;
            this.score = score;
        }
    }
    
    public static class SearchResult {
        public final List<Result> matches = new ArrayList<>();
        public final int skip;
        public final int top;
        public final int total;

        SearchResult () {
            this (0, 0, 0);
        }
        
        SearchResult (int skip, int top, int total) {
            this.skip = skip;
            this.top = top;
            this.total = total;
        }
        public int size () { return matches.size();  }
    }

    public static final SearchResult EMPTY_RESULT = new SearchResult ();

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
            String value = s.substring(pos+1);
            doc.add(new TextField
                    (s.substring(0, pos), value, Field.Store.NO));
            doc.add(new TextField (FIELD_TEXT, value, Field.Store.NO));
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

    public static String queryRewrite (String query) {
        QueryTokenizer tokenizer = new QueryTokenizer (query);
        StringBuilder q = new StringBuilder ();
        for (String tok : tokenizer.tokens()) {
            if (q.length() > 0)
                q.append(" ");
            char ch = tok.charAt(0);
            if (ch == '+' || ch == '-')
                q.append(tok); // as-is
            else if (ch == '~')
                q.append(tok.substring(1)); // optional token
            else if (ch == '"') {
                int slop = 0;
                Matcher m = SLOP.matcher(tok);
                if (m.find()) {
                    slop = Integer.parseInt(tok.substring(m.start()+1));
                    tok = tok.substring(0, m.start());
                }
                
                q.append("+"+tok);
                if (slop > 0)
                    q.append("~"+slop);
            }
            else {
                // parkinson's => +(parkinson parkinson's parkinsons)
                if (tok.endsWith("'s")) {
                    String s = tok.substring(0, tok.length()-2);
                    tok = "+("+tok+" "+s+" "+s+"s)";
                }
                else if (!"AND".equals(tok)
                         && !"OR".equals(tok) && !"NOT".equals(tok)
                         && !tok.endsWith(")")) {
                    q.append("+");
                }
                q.append(tok);
            }
            //Logger.debug("TOKEN: <<"+tok+">>");
        }
        return q.toString();
    }

    public SearchResult search (String query, int max) throws Exception {
        return search (query, 0, max);
    }
    
    public SearchResult search (String query, int skip, int top)
        throws Exception {
        String field = FIELD_TEXT;
        /*
        int pos = query.indexOf(':');
        if (pos > 0) {
            field = query.substring(0, pos);
            query = query.substring(pos+1);
        }
        */
        QueryParser parser = new QueryParser (field, indexWriter.getAnalyzer());
        return search (parser.parse(queryRewrite (query)), skip, top);
    }
    
    public SearchResult search (Query query, int skip, int top)
        throws Exception {
        //System.err.println("** query: "+query);
        try (IndexReader reader = DirectoryReader.open(indexWriter)) {
            IndexSearcher searcher = new IndexSearcher (reader);
            TopDocs hits = searcher.search(query, skip+top);
            SearchResult result = new SearchResult (skip, top, hits.totalHits);
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
                            if (n != null && n.hasLabel(DATA)) {
                                float score = hits.scoreDocs[i].score;
                                /*
                                System.err.println(id+":"+String.format
                                            ("%1$.3f", score)+" "
                                            +n.getAllProperties());
                                */
                                result.matches.add(new Result (n, score));
                            }
                        }
                        catch (NumberFormatException ex) {
                            logger.log(Level.SEVERE, "Bogus id field: "+did,ex);
                        }
                    }
                }
            }
            return result;
        }
    }

    public static Set<String> ngrams (String text, int min, int max)
        throws IOException {
        StandardTokenizer tokenizer = new StandardTokenizer ();

        tokenizer.setReader(new StringReader (text.toLowerCase()));
        StopFilter sf = new StopFilter
            (tokenizer, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        CharTermAttribute token = sf.addAttribute(CharTermAttribute.class);
        StringBuilder sb = new StringBuilder ();
        sf.reset();
        while (sf.incrementToken()) {
            String t = token.toString();
            sb.append(t+" ");
        }
        sf.close();

        tokenizer.setReader(new StringReader (sb.toString()));
        TokenStream ts = new ShingleFilter (tokenizer, min, max);
        token = ts.addAttribute(CharTermAttribute.class);
        ts.reset();
        Set<String> ngrams = new LinkedHashSet<>();
        while (ts.incrementToken()) {
            ngrams.add(token.toString());
        }
        ts.close();
        return ngrams;
    }
}
