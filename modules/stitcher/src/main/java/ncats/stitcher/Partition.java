package ncats.stitcher;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.graph.UnionFind;
import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class Partition {
    static final Logger logger = Logger.getLogger(Partition.class.getName());

    static class SV implements Comparable<SV> {
        final StitchKey key;
        final String value;
        final int count;
        final int total;
        final Set<Long> nodes = new TreeSet<>();

        SV (StitchKey key, String value, int count, int total) {
            this.key = key;
            this.value = value;
            this.count = count;
            this.total = total;
        }

        public int compareTo (SV sv) {
            int d = count - sv.count;
            if (d == 0) {
                d = total - sv.total;
            }
            if (d == 0) {
                d = key.name().compareTo(sv.key.name());
            }
            if (d == 0) {
                d = value.compareTo(sv.value);
            }
            return d;
        }

        public String toString () {
            return key+": \""+value+"\" "+count+"/"+total+" "+nodes;
        }
    }

    static class NV {
        final long id;
        final Set<String> labels = new TreeSet<>();
        final Map<StitchKey, String[]> values = new TreeMap<>();

        NV (long id) {
            this.id = id;
        }
        public String toString () {
            return "["+id+": labels="+labels+" values="+values+"]";
        }
    }

    interface EMStep {
        void step (int k, int i, int j);
    }
    
    static abstract class PLSA {
        final double[][] Pz;
        final double[][] Pw;
        final double[][][] P;

        final int N, M, K, maxiter;
        /*
         * N - number of documents
         * M - number of words (i.e., dictionary size)
         * K - latent dimension
         */
        protected PLSA (int K, int N, int M) {
            Pw = new double[K][M]; // P(w_j | z_k)
            Pz = new double[N][K]; // P(z_k | d_i)
            P = new double[N][M][K]; // P(z_k | d_i, w_j)
            this.K = K;
            this.N = N;
            this.M = M;
            this.maxiter = 10*K;
        }

        protected void init () {
            for (int k = 0; k < K; ++k) {
                for (int i = 0; i < N; ++i) {
                    for (int j = 0; j < M; ++j)
                        P[i][j][k] = Math.random();
                    Pz[i][k] = Math.random();
                }
                
                for (int j = 0; j < M; ++j)
                    Pw[k][j] = Math.random();
            }
        }

        protected void step (EMStep em) {
            for (int k = 0; k < K; ++k) 
                for (int i = 0; i < N; ++i)
                    for (int j = 0; j < M; ++j) {
                        if (hasw (i, j))
                            em.step(k, i, j);
                    }
        }
        
        /*
         * P(z_k|d_i,w_j) = P(w_i|z_k)P(z_k|d_i) 
         *                  / sum_{l..K} P(w_j|z_l)P(z_l|d_i)
         */
        protected void Estep (int k, int i, int j) {
            double Q = 0.;
            for (int l = 0; l < K; ++l)
                Q += Pw[l][j] * Pz[i][l];
            P[i][j][k] = Pw[k][j] * Pz[i][k] / Q;
        }

        protected void Mstep (int k, int i, int j) {
            double num = 0.;
            for (int l = 0; l < N; ++l)
                num += getw (l, j) * P[l][j][k];
            double den = 0.;
            for (int m = 0; m < M; ++m)
                for (int l = 0; l < N; ++l)
                    den += getw (l, m) * P[l][m][k];
            Pw[k][j] = num / den;

            num = 0.;
            for (int m = 0; m < M; ++m)
                num += getw (i, m) * P[i][m][k];
            Pz[i][k] = num / getd (i);
        }

        public int EM () {
            init ();
            int niter = 0;
            for ( ; !converged (niter); ++niter) {
                step (this::Estep);
                step (this::Mstep);
            }
            return niter;
        }

        protected boolean converged (int niter) {
            return niter > maxiter;
        }

        // return true if word j is in document i; false otherwise
        abstract boolean hasw (int i, int j);
        // return the count of word j in document i
        abstract double getw (int i, int j);
        // return the length (number of words) for document i
        abstract double getd (int i);
    }

    class StitchPLSA extends PLSA {
        final NV[] nodes;
        final SV[] values;
        final boolean reversed;

        StitchPLSA (int K, NV[] nodes, SV[] values) {
            super (K, nodes.length, values.length);
            this.nodes = nodes;
            this.values = values;
            reversed = false;
        }

        StitchPLSA (int K, SV[] values, NV[] nodes) {
            super (K, values.length, nodes.length);
            this.nodes = nodes;
            this.values = values;
            reversed = true;
        }

        protected boolean hasw (int i, int j) {
            NV nv;
            SV sv;
            if (reversed) {
                nv = nodes[j];
                sv = values[i];
            }
            else {
                nv = nodes[i];
                sv = values[j];
            }
            return sv.nodes.contains(nv.id);
        }

        protected double getw (int i, int j) {
            return hasw (i, j) ? 1.0 : 0.0;
        }

        protected double getd (int i) {
            double d = 0.;
            if (reversed) {
                d = values[i].nodes.size();
            }
            else {
                NV nv = nodes[i];
                for (Map.Entry<StitchKey, String[]> me : nv.values.entrySet()) {
                    switch (me.getKey()) {
                    case N_Name: case I_CODE:
                        d += me.getValue().length;
                        break;
                    }
                }
            }
            return d;
        }

        public void aspects (OutputStream os) {
            EM ();
            
            PrintStream ps = new PrintStream (os);
            Map<Integer, Set> partitions = new TreeMap<>();
            for (int j = 0; j < M; ++j) {
                Object val;
                if (reversed) {
                    ps.print(String.format("%1$40d", nodes[j].id));
                    val = nodes[j].id;
                }
                else {
                    ps.print(String.format("%1$40s", values[j].value));
                    val = values[j].value;
                }
                
                BitSet set = new BitSet (K);
                double max = 0.; 
                for (int k = 0; k < K; ++k) {
                    ps.print("\t"+String.format("%1$.3f", Pw[k][j]));
                    if (Pw[k][j] < max) {
                    }
                    else if (Pw[k][j] > max) {
                        max = Pw[k][j];
                        set.clear();
                        set.set(k);
                    }
                    else if (max > 0.) {
                        set.set(k);
                    }
                }
                ps.println();
                
                if (set.cardinality() > 1) {
                    logger.warning(val+": "+set);
                }
                else {
                    int k = set.nextSetBit(0);
                    Set mem = partitions.get(k);
                    if (mem == null)
                        partitions.put(k, mem = new LinkedHashSet ());
                    mem.add(val);
                }
            }
            
            for (Map.Entry<Integer, Set> me : partitions.entrySet()) {
                ps.println("++++ K="+me.getKey());
                for (Object v : me.getValue())
                    ps.println(v);
                ps.println();
            }
        }
    }

    Map<String, SV> values = new TreeMap<>();
    Map<Long, NV> nodes = new TreeMap<>();

    public Partition (File file) throws IOException {
        ObjectMapper mapper = new ObjectMapper ();
        try (InputStream is = new FileInputStream (file)) {
            JsonNode json = mapper.readTree(is);

            // stitches
            JsonNode stitches = json.get("stitches");
            if (stitches == null)
                throw new IllegalArgumentException
                    ("Not a valid ncatskg json format!");
            for (Iterator<String> it = stitches.fieldNames(); it.hasNext(); ) {
                String f = it.next();
                try {
                    StitchKey key = StitchKey.valueOf(f);
                    switch (key) {
                    case N_Name: case I_CODE:
                        { JsonNode nodes = stitches.get(f);
                            for (int i = 0; i < nodes.size(); ++i) {
                                JsonNode n = nodes.get(i);
                                SV sv = new SV (key, n.get("value").asText(),
                                                n.get("count").asInt(),
                                                n.get("total").asInt());
                                values.put(sv.value, sv);
                            }
                        }
                    }
                }
                catch (Exception ex) {
                    logger.warning("Not a recognized StitchKey: "+f);
                }
            }

            // nodes
            JsonNode nodes = json.get("nodes");
            if (nodes == null)
                throw new IllegalArgumentException
                    ("Not a valid ncatskg json format!");
            for (int i = 0; i < nodes.size(); ++i) {
                JsonNode n = nodes.get(i);
                long id = n.get("id").asLong();
                NV nv = new NV (id);
                
                JsonNode labels = n.get("labels");
                if (labels != null) {
                    for (int j = 0; j < labels.size(); ++j)
                        nv.labels.add(labels.get(j).asText());
                }
                
                JsonNode props = n.get("properties");
                for (Iterator<String> it = props.fieldNames(); it.hasNext();) {
                    String p = it.next();
                    try {
                        StitchKey key = StitchKey.valueOf(p);
                        JsonNode val = props.get(p);
                        if (val.isArray()) {
                            String[] vals = new String[val.size()];
                            for (int j = 0; j < val.size(); ++j) {
                                String v = val.get(j).asText();
                                SV sv = values.get(v);
                                if (sv != null)
                                    sv.nodes.add(id);
                                vals[j] = v;
                            }
                            nv.values.put(key, vals);
                        }
                        else {
                            String v = val.asText();
                            SV sv = values.get(v);
                            if (sv != null)
                                sv.nodes.add(id);
                            nv.values.put(key, new String[]{v});
                        }
                    }
                    catch (Exception ex) {
                        // not a stitchkey
                    }
                }
                this.nodes.put(id, nv);
            }
            
            logger.info("loading "+json.get("id").asText()+"..."+values.size());
            Set<SV> svs = new TreeSet<>(values.values());
            for (SV sv : svs) {
                System.out.println(sv);
            }
        }
    }
    
    public void plsa (int K) throws Exception {
        plsa (K, System.out);
    }
    
    public void plsa (int K, OutputStream os) throws Exception {
        logger.info("------- VALUE partitions -------");
        StitchPLSA plsa =
            new StitchPLSA (K, nodes.values().toArray(new NV[0]),
                            values.values().toArray(new SV[0]));
        plsa.aspects(os);
        
        logger.info("------- NODE partitions ---------");
        plsa = new StitchPLSA (K, values.values().toArray(new SV[0]),
                               nodes.values().toArray(new NV[0]));
        plsa.aspects(os);
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println
                ("Usage: ncats.stitcher.Partition [K=5] [FILES...");
            System.exit(1);
        }

        int K = 5, i = 0;
        try {
            K = Integer.parseInt(argv[i]);
            ++i;
        }
        catch (NumberFormatException ex) {
        }
        
        for (; i < argv.length; ++i) {
            File file = new File (argv[i]);
            Partition part = new Partition (file);
            part.plsa(K);
        }
    }
}
