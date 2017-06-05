package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URI;
import java.util.zip.*;
import java.util.regex.*;

import java.lang.reflect.Array;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.security.SecureRandom;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Util {
    static final Logger logger = Logger.getLogger(Util.class.getName());
    static final SecureRandom RAND = new SecureRandom ("ncats".getBytes());

    public static final Object NO_CHANGE = new Object ();

    public static class FileStats {
        public final File file;
        public String sha1;
        public long size;

        FileStats (File file) throws IOException {
            byte[] buf = new byte[1024];
            DigestInputStream dis = getDigestInputStream (file);
            size = 0l;
            for (int nb; (nb = dis.read(buf, 0, buf.length)) != -1; )
                size += nb;
            dis.close();
            sha1 = hex (dis.getMessageDigest().digest());
            this.file = file;
        }

        FileStats (URL url) throws IOException {
            file = getLocalFile (url);
            FileOutputStream fos = new FileOutputStream (file);
            DigestInputStream dis = new DigestInputStream
                (url.openStream(), sha1 ());
            byte[] buf = new byte[1024];
            size = 0l;
            for (int nb; (nb = dis.read(buf, 0, buf.length)) != -1; ) {
                fos.write(buf, 0, nb);
                size += nb;
            }
            dis.close();
            fos.close();
            sha1 = hex (dis.getMessageDigest().digest());
        }
    }
    
    private Util () {}

    public static GraphDatabaseService openGraphDb (String file)
        throws IOException {
        return openGraphDb (new File (file));
    }
    
    public static GraphDatabaseService openGraphDb (final File file)
        throws IOException {
        final GraphDatabaseService gdb =
            new GraphDatabaseFactory().newEmbeddedDatabase(file);
        Runtime.getRuntime().addShutdownHook(new Thread() {
                // do shutdown work here
                public void run () {
                    logger.info
                        ("##### Shutting Down Graph Database: "+file+" #####");
                    gdb.shutdown();
                }
            });
        logger.info("##### Open Graph Database: "+file+" ######");
        return gdb;
    }
    
    public static File getLocalFile (URL url) throws IOException {
        try {
            return getLocalFile (url.toURI());
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Bogus URL: "+url, ex);
        }
        return null;
    }
    
    public static File getLocalFile (URI uri) throws IOException {
        File junk = File.createTempFile("_ix", null);
        File dir = junk.getParentFile();
        junk.delete();
        String name = sha1hex(uri.normalize().toString()).substring(0, 11);
        return new File (dir, "_ix"+name);
    }
    
    public static String randString (int len) {
        byte[] buf = new byte[(len+1)/2];
        RAND.nextBytes(buf);
        return hex(buf).substring(0, len);
    }
    
    public static byte[] sha1 (String... contents) {
        MessageDigest md = sha1 ();
        for (String s : contents) {
            try {
                md.update(s.getBytes("utf8"));
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return md.digest();
    }

    public static String sha1hex (byte[] content) {
        return hex (sha1().digest(content));
    }

    public static FileStats fetchFile (URL url) throws IOException {
        return new FileStats (url);
    }
    
    public static FileStats stats (File file) throws IOException {
        return new FileStats (file);
    }
    
    public static byte[] sha1 (File file) throws IOException {
        byte[] buf = new byte[1024];
        DigestInputStream dis = getDigestInputStream (file);
        for (int nb; (nb = dis.read(buf, 0, buf.length)) > 0; )
            ;
        dis.close();
        return dis.getMessageDigest().digest();
    }

    public static String sha1hex (File file) throws IOException {
        return hex (sha1 (file));
    }

    public static String sha1hex (String... contents) {
        return hex (sha1 (contents));
    }

    public static String hex (byte[] data) {
        StringBuilder sb = new StringBuilder ();
        for (int i = 0; i < data.length; ++i) {
            sb.append(String.format("%1$02x", data[i] & 0xff));
        }
        return sb.toString();
    }

    public static MessageDigest sha1 () {
        try {
            return MessageDigest.getInstance("SHA1");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public static byte[] serialize (Object obj) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream ();
        ObjectOutputStream oos = new ObjectOutputStream (bos);
        oos.writeObject(obj);
        oos.close();
        return bos.toByteArray();
    }

    public static String toJson (Object obj) {
        if (obj != null) {
            try {
                ObjectMapper mapper = new ObjectMapper ();
                return mapper.writerWithDefaultPrettyPrinter()
                    .writeValueAsString(obj);           
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't serialize object to json", ex);
            }
        }
        return null;
    }

    public static long[] toPrimitive (Long... values) {
        long[] p = new long[values.length];
        for (int i = 0; i < p.length; ++i)
            p[i] = values[i];
        return p;
    }

    public static double[] toPrimitive (Double... values) {
        double[] p = new double[values.length];
        for (int i = 0; i < p.length; ++i)
            p[i] = values[i];
        return p;
    }

    public static long[] toArray (Collection<Long> values) {
        return Util.toPrimitive(values.toArray(new Long[0]));
    }

    public static String toString (Object obj) {
        return toString (obj, 10);
    }
    
    public static String toString (Object obj, int max) {
        String strval = "";
        if (obj != null) {
            StringBuilder sb = new StringBuilder ();
            if (obj.getClass().isArray()) {
                int len = Array.getLength(obj);
                sb.append("["+len);
                if (len > 0) {
                    int min = Math.min(len, max-5);
                    if (max <= 0) min = len;
                    for (int i = 0; i < min; ++i) {
                        Object v = Array.get(obj, i);
                        sb.append(","+(v != null
                                       ? "\""+v.toString()+"\"" : ""));
                    }
                    
                    if (len > min) {
                        Object v = Array.get(obj, len-1);
                        sb.append(",...,"+(v != null
                                           ? "\""+v.toString()+"\"": ""));
                    }
                }
                sb.append("]");
            }
            else
                sb.append("\""+obj.toString()+"\"");
            
            strval = sb.toString();
        }
        return strval;
    }

    public static DigestInputStream getDigestInputStream (String file)
        throws FileNotFoundException, IOException {
        return getDigestInputStream (new File (file));
    }

    public static DigestInputStream getDigestInputStream (File file)
        throws FileNotFoundException, IOException {
        try {
            return new DigestInputStream
                (new GZIPInputStream (new FileInputStream (file)),
                 sha1 ());
        }
        catch (ZipException ex) {
            // now let's read as raw..
            return new DigestInputStream
                (new FileInputStream (file), sha1 ());
        }
    }

    public static Set toSet (Object value) {
        Set set = new HashSet ();
        if (value != null) {
            if (value.getClass().isArray())
                for (int i = 0; i < Array.getLength(value); ++i)
                    set.add(Array.get(value, i));
            else
                set.add(value);
        }
        return set;
    }

    public static Object[] toArray (Object value) {
        Object[] array = {};
        if (value != null) {
            if (value.getClass().isArray()) {
                array = new Object[Array.getLength(value)];
                for (int i = 0; i < array.length; ++i)
                    array[i] = Array.get(value, i);
            }
            else {
                array = new Object[]{value};
            }
        }
        return array;
    }
    
    public static Object merge (Object... values) {
        Class type = null;
        Set unique = new HashSet ();
        for (Object val : values) {
            if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (type == null)
                        type = v.getClass();
                    else if (!v.getClass().isAssignableFrom(type))
                        throw new IllegalArgumentException
                            ("Incompatible class; "+v.getClass().getName()
                             +" is not assignable from "+type.getName());
                    unique.add(v);
                }
            }
            else {
                if (type == null)
                    type = val.getClass();
                unique.add(val);
            }
        }
        
        Object merged = Array.newInstance(type, unique.size());
        int count = 0;
        for (Object val : values) {
            if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (unique.remove(v))
                        Array.set(merged, count++, v);
                }
            }
            else if (unique.remove(val)) {
                Array.set(merged, count++, val);
            }
        }
        return merged;
    }

    static public Object delta (Object val, Object old) {
        Class type = null;
        Set oldSet = new HashSet ();
        if (old.getClass().isArray()) {
            int len = Array.getLength(old);
            for (int i = 0; i < len; ++i) {
                Object v = Array.get(old, i);
                if (type == null)
                    type = v.getClass();
                else if (!v.getClass().isAssignableFrom(type))
                    throw new IllegalArgumentException
                        ("Incompatible class; "+v.getClass().getName()
                             +" is not assignable from "+type.getName());
                oldSet.add(v);
            }
        }
        else {
            type = old.getClass();
            oldSet.add(old);
        }

        Set unique = new HashSet();
        if (val.getClass().isArray()) {
            int len = Array.getLength(val);
            for (int i = 0; i < len; ++i) {
                Object v = Array.get(val, i);
                if (!v.getClass().isAssignableFrom(type))
                    throw new IllegalArgumentException
                        ("Incompatible class; "+v.getClass().getName()
                         +" is not assignable from "+type.getName());
                unique.add(v);
            }
        }
        else {
            unique.add(val);
        }
        int size = unique.size();
        unique.removeAll(oldSet);

        // can't distinguish between no change and complete removal!!!
        Object delta = null;
        if (!unique.isEmpty()) {
            if (unique.size() < size) {
                delta = Array.newInstance(type, unique.size());
                int n = 0;
                for (Object v : unique)
                    Array.set(delta, n++, v);
            }
            else
                delta = NO_CHANGE;
        }
        return delta;
    }

    public static <T> T[] reverse (T... array) {
        int i = 0, j = array.length-1;
        while (i < j) {
            T temp = array[i];
            array[i] = array[j];
            array[j] = temp;
            ++i;
            --j;
        }
        return array;
    }
    
    /*
     * parse SRS json into a Molecule
     */
    public static Molecule fromJson (String json) {
        ObjectMapper mapper = new ObjectMapper ();
        Molecule mol = null;
        try {
            JsonNode node = mapper.readTree(json);
            JsonNode struc = node.get("structure");
            if (struc != null) {
                JsonNode molfile = struc.get("molfile");
                if (molfile != null) {
                    MolHandler mh = new MolHandler (molfile.asText());
                    mol = mh.getMolecule();
                    
                    JsonNode unii = node.get("approvalID");
                    if (unii != null) {
                        mol.setProperty("UNII", unii.asText());
                        mol.setName(unii.asText());
                    }
                    
                    JsonNode names = node.get("names");
                    if (names != null && names.isArray()) {
                        int size = names.size();
                        StringBuilder sb = new StringBuilder ();
                        for (int i = 0; i < size; ++i) {
                            JsonNode n = names.get(i);
                            String name = n.get("name").asText();
                            if (sb.length() > 0) sb.append("\n");
                            sb.append(name);
                        }
                        
                        mol.setProperty("Synonyms", sb.toString());
                    }
                    
                    JsonNode codes = node.get("codes");
                    if (codes != null && codes.isArray()) {
                        int size = codes.size();
                        Map<String, StringBuilder> buf =
                            new HashMap<String, StringBuilder>();
                        for (int i = 0; i < size; ++i) {
                            JsonNode n = codes.get(i);
                            if ("PRIMARY".equals(n.get("type").asText())) {
                                String sys = n.get("codeSystem").asText();
                                StringBuilder sb = buf.get(sys);
                                if (sb == null) {
                                    buf.put(sys, sb = new StringBuilder ());
                                }
                                if (sb.length() > 0) sb.append("\n");
                                sb.append(n.get("code").asText());
                            }
                        }
                        
                        for (Map.Entry<String, StringBuilder> me
                                 : buf.entrySet()) {
                            mol.setProperty(me.getKey(),
                                            me.getValue().toString());
                        }
                    }

                    JsonNode refs = node.get("references");
                    if (refs != null && refs.isArray()) {
                        int size = refs.size();
                        StringBuilder sb = new StringBuilder ();
                        for (int i = 0; i < size; ++i) {
                            JsonNode n = refs.get(i);
                            if (sb.length() > 0) sb.append("\n");
                            sb.append(n.get("citation").asText());
                        }
                        mol.setProperty("References", sb.toString());
                    }

                    JsonNode rels = node.get("relationships");
                    if (rels != null && rels.isArray()) {
                        StringBuilder sb = new StringBuilder ();
                        for (int i = 0; i < rels.size(); ++i) {
                            JsonNode n = rels.get(i);
                            String type = n.get("type").asText();
                            if ("ACTIVE MOIETY".equalsIgnoreCase(type)) {
                                String id = n.get("relatedSubstance")
                                    .get("approvalID").asText();
                                if (sb.length() > 0) sb.append("\n");
                                sb.append(id);
                            }
                        }
                        
                        if (sb.length() > 0)
                            mol.setProperty("ActiveMoieties", sb.toString());
                    }
                }
                else
                    logger.warning("No \"molfile\" found in \"structure\"!");
            }
            else {
                logger.warning("No \"structure\" found in json!");
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse json", ex);
        }
        return mol;
    }

    public static JsonNode toJsonNode (String...values) {
        ObjectMapper mapper = new ObjectMapper ();
        ArrayNode array = mapper.createArrayNode();
        if (values != null) {
            for (String v : values) 
                array.add(v);
        }
        return array;
    }

    public static String[] tokenizer (String line, String delim) {
        return delim.length() == 1 ? tokenizer (line, delim.charAt(0))
            : line.split(delim);
    }
    
    public static String[] tokenizer (String line, char delim) {
        List<String> toks = new ArrayList<String>();

        int len = line.length(), parity = 0;
        StringBuilder curtok = new StringBuilder ();
        for (int i = 0; i < len; ++i) {
            char ch = line.charAt(i);
            if (ch == '"') {
                parity ^= 1;
            }
            if (ch == delim) {
                if (parity == 0) {
                    String tok = null;
                    if (curtok.length() > 0) {
                        tok = curtok.toString();
                    }
                    toks.add(tok);
                    curtok.setLength(0);
                }
                else {
                    curtok.append(ch);
                }
            }
            else if (ch != '"') {
                curtok.append(ch);
            }
        }

        if (curtok.length() > 0) {
            toks.add(curtok.toString());
        }
        // line ends in the delim character, so we add one more value
        else if (line.charAt(len-1) == delim)
            toks.add(null);

        return toks.toArray(new String[0]);
    }

    static Pattern FloatRegex =
        Pattern.compile("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$");
    static Pattern IntegerRegex = Pattern.compile("^[-+]?[0-9]+$");
    
    public static Class typeInference (String token) {
        if (token == null || token.length() == 0)
            return String.class;
        
        Matcher m = IntegerRegex.matcher(token);
        if (m.matches())
            return Long.class;

        m = FloatRegex.matcher(token);
        if (m.matches())
            return Double.class;

        // see if might be a structure
        try {
            MolHandler mh = new MolHandler (token);
            Molecule mol = mh.getMolecule();
            return Molecule.class;
        }
        catch (Exception ex) {
        }

        // ok, give up
        return String.class;
    }

    public static String sha1 (Collection<Long> ids) {
        MessageDigest sha1 = sha1 ();
        byte[] data = new byte[8];
        for (Long id : ids) {
            data[0] = (byte)((id >> 56) & 0xff);
            data[1] = (byte)((id >> 48) & 0xff);
            data[2] = (byte)((id >> 40) & 0xff);
            data[3] = (byte)((id >> 32) & 0xff);
            data[4] = (byte)((id >> 24) & 0xff);
            data[5] = (byte)((id >> 16) & 0xff);
            data[6] = (byte)((id >> 8) & 0xff);
            data[7] = (byte)(id & 0xff);
            sha1.update(data);
        }
        return hex (sha1.digest());
    }

    public static void dump (Component component) {
        dump (System.out, component);
    }
    
    public static void dump (OutputStream os, Component component) {
        ObjectMapper mapper = new ObjectMapper ();
        PrintStream ps = new PrintStream (os);
        ps.println
            ("+++++++ Component "+component.getId()+" +++++++");
        ps.println("nodes="+component.size() + " "+component.nodeSet());
        /*
        try {
            for (Entity e : component.entities()) {
                ps.println("  "+e.getId()+": "
                           +mapper.writerWithDefaultPrettyPrinter()
                           .writeValueAsString(e.keys()));
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
            }*/
        
        ps.println("-- stitches --");
        for (StitchKey key : EnumSet.allOf(StitchKey.class)) {
            final Map<Object, Integer> stats = component.stats(key);
            if (!stats.isEmpty()) {
                Set<Object> sorted = new TreeSet<>(new Comparator () {
                        public int compare (Object o1, Object o2) {
                            int d = stats.get(o2) - stats.get(o1);
                            if (d == 0)
                                d = o1.toString().compareTo(o2.toString());
                            return d;
                        }
                    });
                sorted.addAll(stats.keySet());
                
                ps.println(key+"="+stats.size());
                for (Object v : sorted)
                    ps.println("  "+v + ": "+stats.get(v));
            }
        }
        ps.println();
    }

    public static void dump (Clique clique) {
        dump (System.out, clique);
    }
    
    public static void dump (OutputStream os, Clique clique) {
        PrintStream ps = new PrintStream (os);
        ps.println
            ("+++++++ Clique "+clique.getId()+" +++++++");
        ps.println("size: "+clique.size());
        ps.println(String.format("score: %1$.3f",
                                         clique.score()));
        if (clique.size() > 0) {
            Entity e = clique.entities()[0].parent();
            ps.println("parent: " +e.getId()
                               +" ("+e.get(Props.RANK)+")");
            ps.print("nodes: [");
            int i = 0;
            for (Entity n : clique) {
                ps.print(n.getId());
                if (++i < clique.size())
                    ps.print(",");
            }
            ps.println("]");
        }

        ps.println("-- stitch keys --");
        for (Map.Entry<StitchKey, Object> me
                 : clique.values().entrySet()) {
            ps.print(me.getKey()+":");
            Object val = me.getValue();
            if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    ps.print(" "+Array.get(val, i));
                }
            }
            else
                ps.print(" "+val);
            ps.println();
        }
        ps.println();
    }
}
