package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.*;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.*;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import org.apache.lucene.store.*;
import org.apache.lucene.analysis.*;
import org.apache.lucene.index.*;
import org.apache.lucene.document.*;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.search.*;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.vectorhighlight.FastVectorHighlighter;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.core.StopAnalyzer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tr.ApostropheFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;

import org.apache.lucene.search.suggest.*;
import org.apache.lucene.search.suggest.tst.*;
import org.apache.lucene.search.suggest.fst.FSTCompletionLookup;
import org.apache.lucene.search.suggest.analyzing.FreeTextSuggester;
import org.apache.lucene.search.suggest.analyzing.AnalyzingInfixSuggester;

import static ncats.stitcher.StitchKey.*;

public class TextResolver implements AutoCloseable {
    static final Logger logger =
        Logger.getLogger(TextResolver.class.getName());

    static final String FIELD_TEXT = "text";
    static final int MAX_HITS = 100; // max hits per query
    static final int MIN_LENGTH = 3;
    static final int NUM_THREADS = 4;

    static final Analyzer ANALYZER =
        // new StandardAnalyzer ();
        new CustomAnalyzer ();

    static class CustomAnalyzer extends StopwordAnalyzerBase {
        public CustomAnalyzer () {
            super (StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        }

        @Override
        protected TokenStreamComponents createComponents
            (final String fieldName) {
            Tokenizer tokenizer = new StandardTokenizer ();
            TokenStream ts = new StandardFilter (tokenizer);
            ts = new LowerCaseFilter (ts);
            ts = new StopFilter (ts, stopwords);
            ts = new ApostropheFilter (ts);
            return new TokenStreamComponents (tokenizer, ts);
        }
    }
    
    static class QueryString {
        public final String text;
        public final Query query;
        
        QueryString (String text) throws Exception {
            StandardTokenizer tokenizer = new StandardTokenizer ();
            tokenizer.setReader(new StringReader (text.toLowerCase()));
            StopFilter sf = new StopFilter
                (tokenizer, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
            CharTermAttribute token = sf.addAttribute(CharTermAttribute.class);
            StringBuilder sb = new StringBuilder ();
            sf.reset();
            int ntoks = 0;
            while (sf.incrementToken()) {
                String t = token.toString();
                if (ntoks > 0)
                    sb.append(" ");
                sb.append(t);
                ++ntoks;
            }
            sf.close();
            this.text = text;
            String tq = "\""+sb+"\"~"+Math.max(1, (ntoks+1)/2);
            QueryParser qp = new QueryParser (FIELD_TEXT, ANALYZER);
            this.query = qp.parse(tq);
        }

        public String toString () {
            return "{"+text+" => "+query+"}";
        }
    }

    static class NodeQuery {
        static final NodeQuery EMPTY = new NodeQuery ();
        
        final StitchKey key;
        final long id;
        final String text;
        final Query query;

        NodeQuery () {
            this (0, null, null, null);
        }
        NodeQuery (long id, StitchKey key, String text, Query query) {
            this.id = id;
            this.key = key;
            this.text = text;
            this.query = query;
        }
    }

    class Hit implements Comparable<Hit> {
        public final Set<Long> nodes = new LinkedHashSet<>();
        public final Document doc;
        public final float score;
        public final String value;
        public final String[] fragments;

        Hit (float score, Document doc, String value, String[] fragments) {
            this.score = score;
            this.doc = doc;
            this.value = value;
            List<String> frags = new ArrayList<>();
            for (String f : fragments) {
                int next = 0, pos;
                // merge <b>GD</b> type <b>2</b> to <b>GD type 2</b>
                StringBuilder sb = new StringBuilder ();
                do {
                    pos = f.indexOf("</b>", next);
                    sb.append(f.substring(next, pos));
                    next = f.indexOf("<b>", pos+4);
                    if (next < 0) {
                        sb.append(f.substring(pos));
                        break;
                    }
                    else {
                        sb.append(f.substring(pos+4, next));
                        next += 3;
                    }
                }
                while (true);
                frags.add(sb.toString());
            }
            this.fragments = frags.toArray(new String[0]);
        }

        void add (Node node) {
            this.nodes.add(node.getId());
        }
        
        void add (Long id) {
            this.nodes.add(id);
        }

        public int compareTo (Hit h) {
            if (h.score > score) return 1;
            if (h.score < score) return -1;
            return 0;
        }
    }

    final static FieldType tvFieldType = new FieldType (TextField.TYPE_STORED);
    static {
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();
    }

    class SearchWorker implements Runnable {
        public void run () {
            try {
                int count = 0;
                for (NodeQuery nq;
                     (nq = queue.take()) != NodeQuery.EMPTY; ++count) {
                    search (nq);
                }
                logger.info("### "+Thread.currentThread()+": "
                            +count+" queries processed!");
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "** "
                           +Thread.currentThread()+": search failed!", ex);
            }
        }
    }
    
    final Directory indexDir;
    final IndexReader reader;
    final IndexSearcher searcher;
    final FastVectorHighlighter fvh = new FastVectorHighlighter (true, true);
    final BlockingQueue<Hit> bestHits = new PriorityBlockingQueue<>(20);
    final ConcurrentMap<String, List<Hit>> values = new ConcurrentHashMap<>();
    final BlockingQueue<NodeQuery> queue = new ArrayBlockingQueue<>(100);
    final ExecutorService es = Executors.newFixedThreadPool(NUM_THREADS);
        
    public TextResolver (String text) throws IOException {
        indexDir = new RAMDirectory ();
        IndexWriter writer = new IndexWriter
            (indexDir, new IndexWriterConfig (ANALYZER));

        Document doc = new Document ();
        doc.add(new Field (FIELD_TEXT, text, tvFieldType));
        writer.addDocument(doc);
        writer.close();
        
        reader = DirectoryReader.open(indexDir);
        searcher = new IndexSearcher (reader);
        for (int i = 0; i < NUM_THREADS; ++i)
            es.submit(new SearchWorker ());
    }

    public void close () throws Exception {
        IOUtils.close(reader, indexDir);
        for (int i = 0; i < NUM_THREADS; ++i)
            queue.put(NodeQuery.EMPTY);
        es.shutdown();
    }

    void search (NodeQuery q) throws Exception {
        int len = q.text.length();
        if (len > MIN_LENGTH) {
            if (!values.containsKey(q.text)) {
                FastVectorHighlighter fvh = new FastVectorHighlighter ();
                FieldQuery fq = fvh.getFieldQuery(q.query, reader);
                TopDocs docs = searcher.search(q.query, MAX_HITS);
                
                if (docs.scoreDocs.length > 0) {
                    List<Hit> hits = new ArrayList<>();
                    for (int i = 0; i < docs.scoreDocs.length; ++i) {
                        int docId = docs.scoreDocs[i].doc;
                        float score = docs.scoreDocs[i].score;
                        Document doc = searcher.doc(docId);
                        String[] frags = fvh.getBestFragments
                            (fq, reader, docId, FIELD_TEXT,
                             Math.max(30, len), 10);
                        Hit h = new Hit (score, doc, q.text, frags);
                        if (!bestHits.offer(h)) {
                            logger.log(Level.WARNING,
                                       "Can't insert hit into queue!");
                        }
                        else {
                            h.add(q.id);
                            hits.add(h);
                        }
                    }
                    values.computeIfAbsent
                        (q.text, k -> new ArrayList<>()).addAll(hits);
                }
            }
            else {
                values.get(q.text).forEach(h -> h.add(q.id));
            }
        }
    }
    
    void addDict (PrintStream ps, Node node, StitchKey key, String text)
        throws Exception {
        StandardTokenizer tokenizer = new StandardTokenizer ();
        tokenizer.setReader(new StringReader (text.toLowerCase()));
        StopFilter sf = new StopFilter
            (tokenizer, StopAnalyzer.ENGLISH_STOP_WORDS_SET);
        CharTermAttribute token = sf.addAttribute(CharTermAttribute.class);
        sf.reset();
        int ntoks = 0;
        while (sf.incrementToken()) {
            String t = token.toString();
            if (ntoks > 0)
                ps.print(" ");
            ps.print(t);
            ++ntoks;
        }
        sf.close();
        ps.print("\t");
        int wt = 1;
        for (Relationship rel : node.getRelationships(key)) {
            if (rel.hasProperty(Props.VALUE)
                && text.equals(rel.getProperty(Props.VALUE))) {
                ++wt;
            }
        }
        ps.println(wt+"\t"+node.getId());
    }
    
    void dict (StitchKey key, ResourceIterator<Node> nodes)
        throws Exception {
        try (FileOutputStream fos = new FileOutputStream (key.name()+".txt")) {
            PrintStream ps = new PrintStream (fos);
            
            while (nodes.hasNext()) {
                Node node = nodes.next();
                if (node.hasProperty(key.name())) {
                    Object value = node.getProperty(key.name());
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        for (int i = 0; i < len; ++i) {
                            Object v = Array.get(value, i);
                            if (v instanceof String)
                                addDict (ps, node, key, (String)v);
                            else
                                break;
                        }
                    }
                    else if (value instanceof String) {
                        addDict (ps, node, key, (String)value);
                    }
                }
            }
        }
        finally {
            nodes.close();
        }
    }

    static void dictLookup (String s, Lookup lookup) throws Exception {
        System.err.println("### looking up..."+s);
        List<Lookup.LookupResult> matches = lookup.lookup(s, null, true, 10);
        for (Lookup.LookupResult r : matches) {
            System.err.println("..."+r.key+" "
                               +Long.parseLong(r.payload.utf8ToString()));
        }
    }
    
    void buildDict () throws Exception {
        try (FileInputStream fis = new FileInputStream ("N_Name.txt")) {
            FileDictionary fdict = new FileDictionary (fis);
            AnalyzingInfixSuggester lookup =
                new AnalyzingInfixSuggester (new RAMDirectory(), ANALYZER);
            lookup.build(fdict.getEntryIterator());
            
            System.err.println("** "+lookup.getCount()+" dict entries loaded!");
            dictLookup ("gaucher's", lookup);
            dictLookup ("gd type 1", lookup);
            dictLookup ("disease", lookup);
        }
    }

    void search () throws Exception {
        try (FileInputStream fis = new FileInputStream ("N_Name.txt")) {
            FileDictionary fdict = new FileDictionary (fis);
            InputIterator iter = fdict.getEntryIterator();
            QueryParser qp = new QueryParser (FIELD_TEXT, ANALYZER);
            for (BytesRef bref; (bref = iter.next()) != null; ) {
                String text = bref.utf8ToString();
                //System.err.println("***"+text);
                int len = text.length();
                if (len > 0) {
                    Query query = qp.parse("\""+text+"\"~"+Math.max(1, 3));
                    NodeQuery nq = new NodeQuery
                        (Long.parseLong(iter.payload().utf8ToString()),
                         N_Name, text, query);
                    queue.put(nq);
                }
            }
        }
    }
    
    public void resolve (Node node, StitchKey key, String value)
        throws Exception {
        int len = value.length();
        if (len > MIN_LENGTH && !values.containsKey(value)) {
            QueryString qs = new QueryString (value);
            //System.err.println("**** "+qs);
            
            FieldQuery fq = fvh.getFieldQuery(qs.query, reader);
            TopDocs docs = searcher.search(qs.query, MAX_HITS);
            if (docs.scoreDocs.length > 0) {
                List<Hit> hits = new ArrayList<>();
                for (int i = 0; i < docs.scoreDocs.length; ++i) {
                    int docId = docs.scoreDocs[i].doc;
                    float score = docs.scoreDocs[i].score;
                    Document doc = searcher.doc(docId);
                    String[] frags = fvh.getBestFragments
                        (fq, reader, docId, FIELD_TEXT, Math.max(30, len), 10);
                    Hit h = new Hit (score, doc, value, frags);
                    if (!bestHits.offer(h)) {
                        logger.log(Level.WARNING,
                                   "Can't insert hit into queue!");
                    }
                    else {
                        h.add(node);
                        hits.add(h);
                    }
                }
                // only keep track of values already matched; otherwise
                // this hash will become very large!
                values.computeIfAbsent
                    (value, k -> new ArrayList<>()).addAll(hits);
            }
        }
    }

    public void resolve (Node node, StitchKey... keys) throws Exception {
        if (keys.length == 0)
            keys = new StitchKey[]{
                N_Name
            };
        
        for (StitchKey k : keys) {
            String prop = k.name();
            if (node.hasProperty(prop)) {
                Object value = node.getProperty(prop);
                if (value.getClass().isArray()) {
                    int len = Array.getLength(value);
                    for (int i = 0; i < len; ++i) {
                        Object v = Array.get(value, i);
                        if (v instanceof String) {
                            resolve (node, k, (String)v);
                        }
                        else
                            break;
                    }
                }
                else if (value instanceof String) {
                    resolve (node, k, (String)value);
                }
            }
        }
    }

    public void resolve (ResourceIterator<Node> nodes, StitchKey... keys)
        throws Exception {
        try {
            if (keys.length == 0)
                keys = new StitchKey[]{
                    N_Name
                };
            int count = 0;
            while (nodes.hasNext()) {
                Node node = nodes.next();
                resolve (node, keys);
                ++count;
            }
        }
        finally {
            nodes.close();
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: TextResolver DB");
            System.exit(1);
        }
        String text = "Gaucher disease (GD) encompasses a continuum of clinical findings from a perinatal lethal disorder to an asymptomatic type. The identification of three major clinical types (1, 2, and 3) and two other subtypes (perinatal-lethal and cardiovascular) is useful in determining prognosis and management. GD type 1 is characterized by the presence of clinical or radiographic evidence of bone disease (osteopenia, focal lytic or sclerotic lesions, and osteonecrosis), hepatosplenomegaly, anemia and thrombocytopenia, lung disease, and the absence of primary central nervous system disease. GD types 2 and 3 are characterized by the presence of primary neurologic disease; in the past, they were distinguished by age of onset and rate of disease progression, but these distinctions are not absolute. Disease with onset before age two years, limited psychomotor development, and a rapidly progressive course with death by age two to four years is classified as GD type 2. Individuals with GD type 3 may have onset before age two years, but often have a more slowly progressive course, with survival into the third or fourth decade. The perinatal-lethal form is associated with ichthyosiform or collodion skin abnormalities or with nonimmune hydrops fetalis. The cardiovascular form is characterized by calcification of the aortic and mitral valves, mild splenomegaly, corneal opacities, and supranuclear ophthalmoplegia. Cardiopulmonary complications have been described with all the clinical subtypes, although varying in frequency and severity.";
        try (EntityFactory ef = new EntityFactory (argv[0]);
             TextResolver resolver = new TextResolver (text)) {
            //resolver.resolve(null, N_Name, "Gaucher's disease");
            
            GraphDatabaseService gdb = ef.getGraphDb().graphDb();
            try (Transaction tx = gdb.beginTx()) {
                //resolver.resolve(gdb.findNodes(AuxNodeType.ENTITY));
                resolver.search();
                tx.success();
            }

            System.err.println("** resolving..."+text.length()+"\n"+text);
            List<Hit> hits = new ArrayList<>();
            resolver.bestHits.drainTo(hits);
            for (Hit h : hits) {
                System.err.print("### "+h.nodes+": ");
                System.err.println(String.format("%1$.3f", h.score)
                                   + " \""+h.value+"\"");
                for (String f : h.fragments) {
                    System.err.println("......\""+f+"\"");
                }
            }
        }
    }
}
