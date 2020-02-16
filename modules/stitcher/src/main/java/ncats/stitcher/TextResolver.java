package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.BlockingQueue;

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

import static ncats.stitcher.StitchKey.*;

public class TextResolver implements AutoCloseable {
    static final Logger logger =
        Logger.getLogger(TextResolver.class.getName());

    static final String FIELD_TEXT = "text";
    static final int MAX_HITS = 100; // max hits per query
    static final int MIN_LENGTH = 3;

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
                sb.append(t+" ");
                ++ntoks;
            }
            sf.close();
            this.text = text;
            String tq = "\""+sb.toString().trim()
                +"\"~"+(1+Math.max(1, ntoks/2));
            QueryParser qp = new QueryParser
                (FIELD_TEXT, new StandardAnalyzer ());
            this.query = qp.parse(tq);
        }

        public String toString () {
            return "{"+text+" => "+query+"}";
        }
    }

    class Hit implements Comparable<Hit> {
        public final Set<Node> nodes = new LinkedHashSet<>();
        public final float score;
        public final String value;
        public final String[] fragments;

        Hit (float score, String value, String[] fragments) {
            this.score = score;
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
            this.nodes.add(node);
            allhits.computeIfAbsent(node, k -> new ArrayList<>()).add(this);
        }

        public int compareTo (Hit h) {
            if (h.score > score) return 1;
            if (h.score < score) return -1;
            return 0;
        }
    }

    final FieldType tvFieldType;
    final Directory indexDir;
    final IndexReader reader;
    final IndexSearcher searcher;
    final StitchKey[] keys;
    final FastVectorHighlighter fvh = new FastVectorHighlighter (true, true);
    final BlockingQueue<Hit> bestHits = new PriorityBlockingQueue<>(20);
    final Map<Node, List<Hit>> allhits = new HashMap<>();
    final Set<String> values = new HashSet<>();
        
    public TextResolver (String text) throws IOException {
        this (text, N_Name);
    }

    public TextResolver (String text, StitchKey... keys) throws IOException {
        indexDir = new RAMDirectory ();
        IndexWriter writer = new IndexWriter
            (indexDir, new IndexWriterConfig (new StandardAnalyzer ()));
        tvFieldType = new FieldType (TextField.TYPE_STORED);
        tvFieldType.setStoreTermVectors(true);
        tvFieldType.setStoreTermVectorPositions(true);
        tvFieldType.setStoreTermVectorPayloads(true);
        tvFieldType.setStoreTermVectorOffsets(true);
        tvFieldType.freeze();

        Document doc = new Document ();
        doc.add(new Field (FIELD_TEXT, text, tvFieldType));
        writer.addDocument(doc);
        writer.close();
        
        reader = DirectoryReader.open(indexDir);
        searcher = new IndexSearcher (reader);
        this.keys = keys;
    }

    public void close () throws Exception {
        IOUtils.close(reader, indexDir);
    }

    void transitive (Node node, StitchKey key, Object value, Hit h) {
        h.add(node);
        for (Relationship rel : node.getRelationships(key)) {
            if (rel.hasProperty(Props.VALUE)
                && value.equals(rel.getProperty(Props.VALUE))) {
                Node n = rel.getOtherNode(node);
                if (!allhits.containsKey(n))
                    transitive (n, key, value, h);
            }
        }
    }

    public void resolve (Node node, StitchKey key, String value) {
        int len = value.length();
        if (len > MIN_LENGTH && !values.contains(value)) {
            try {
                QueryString qs = new QueryString (value);
                //System.err.println("**** "+qs);
                
                FieldQuery fq = fvh.getFieldQuery(qs.query, reader);
                TopDocs docs = searcher.search(qs.query, MAX_HITS);
                if (docs.scoreDocs.length > 0) {
                    for (int i = 0; i < docs.scoreDocs.length; ++i) {
                        int docId = docs.scoreDocs[i].doc;
                        float score = docs.scoreDocs[i].score;
                        //Document doc = searcher.doc(docId);
                        String[] frags = fvh.getBestFragments
                            (fq, reader, docId, FIELD_TEXT,
                             Math.max(30, len), 10);
                        Hit h = new Hit (score, value, frags);
                        if (!bestHits.offer(h)) {
                            logger.log(Level.WARNING,
                                       "Can't insert hit into queue!");
                        }
                        else {
                            transitive (node, key, value, h);
                        }
                    }
                    // only keep track of values already matched; otherwise
                    // this hash will become very large!
                    values.add(value);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't parse query: "+value, ex);
            }
        }
    }

    public void resolve (ResourceIterator<Node> nodes) throws Exception {
        try {
            while (nodes.hasNext()) {
                Node node = nodes.next();
                /*if (!allhits.containsKey(node))*/ {
                    for (StitchKey k : keys) {
                        Object value = node.getProperty(k.name());
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
            //resolver.resolve(null, "Gaucher disease");
            
            GraphDatabaseService gdb = ef.getGraphDb().graphDb();
            try (Transaction tx = gdb.beginTx()) {
                resolver.resolve(gdb.findNodes(AuxNodeType.ENTITY));
                tx.success();
            }

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
