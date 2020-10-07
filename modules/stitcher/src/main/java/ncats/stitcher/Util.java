package ncats.stitcher;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URI;
import java.util.stream.Stream;
import java.util.zip.*;
import java.util.regex.*;
import java.util.function.Predicate;

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
import chemaxon.struc.MolBond;
import chemaxon.struc.MolAtom;
import chemaxon.util.MolHandler;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.Index;
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

    public static String encode64 (String s) throws Exception {
        return encode64 (s, false);
    }
    
    public static String encode64 (String s, boolean compress)
        throws Exception {
        byte[] b = s.getBytes("utf8");
        if (compress) {
            Deflater compresser = new Deflater ();
            compresser.setInput(b);
            compresser.finish();
            ByteArrayOutputStream bos = new ByteArrayOutputStream ();
            byte[] buf = new byte[1024];
            for (int len; (len = compresser.deflate(buf)) > 0; ) {
                bos.write(buf, 0, len);
            }
            b = bos.toByteArray();
        }
        return Base64.getEncoder().encodeToString(b);
    }

    public static String decode64 (byte[] b) throws Exception {
        return decode64 (b, false);
    }

    public static String decode64 (String s) throws Exception {
        return decode64 (s.getBytes("utf8"));
    }

    public static String decode64 (String s, boolean compressed)
        throws Exception {
        return decode64 (s.getBytes("utf8"), compressed);
    }
    
    public static String decode64 (byte[] b, boolean compressed)
        throws Exception {
        b = Base64.getDecoder().decode(b);
        if (compressed) {
            Inflater in = new Inflater ();
            in.setInput(b);
            byte[] buf = new byte[1024];
            ByteArrayOutputStream bos = new ByteArrayOutputStream ();
            for (int len; (len = in.inflate(buf)) > 0; ) {
                bos.write(buf, 0, len);
            }
            in.end();
            b = bos.toByteArray();
        }
        return new String (b);
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

    public static String toString (Map values) {
        StringBuilder sb = new StringBuilder ("{");
        int i = 0;
        for (Object obj : values.entrySet()) {
            Map.Entry me = (Map.Entry)obj;
            sb.append(me.getKey()+"="+toString (me.getValue()));
            if (++i < values.size()) sb.append(" ");
        }
        sb.append("}");
        return sb.toString();
    }
    
    public static String toString (Object obj) {
        return toString (obj, 10);
    }

    public static int getLength (Object obj) {
        if (obj == null) return 0;
        if (obj instanceof Map)
            return ((Map)obj).size();
        if (obj instanceof Collection)
            return ((Collection)obj).size();
        if (obj.getClass().isArray())
            return Array.getLength(obj);
        return 1;
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
        for (Object val : values) {
            if (val == null)
                ;
            else if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (v != null)
                        if (type == null)
                            type = v.getClass();
                        else if (!v.getClass().isAssignableFrom(type))
                            type = String.class; // Use String class as a common demoninator here as we are mostly using this for combining primitive types
                    // Problem was coming 2 PubChem CIDs, one provided as a long from Rancho (ALAFOSFALIN; 71957) and the other as a string from GSRS (XMK47YQG9R; 12757032)
                    //                        throw new IllegalArgumentException
                    //     ("Incompatible class; "+v.getClass().getName()
                    //       +" is not assignable from "+type.getName()+": "+v+"|"+val+":"+Arrays.toString(unique.toArray()));
                }
            }
            else {
                if (type == null)
                    type = val.getClass();
                else if (!val.getClass().isAssignableFrom(type))
                    type = String.class;
            }
        }

        Set unique = new HashSet();
        for (Object val : values) {
            if (val == null)
                ;
            else if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (v != null)
                        unique.add(type == String.class
                                   ? v.toString() : type.cast(v));
                    else {
                        v = null;
                    }
                }
            }
            else {
                unique.add(type == String.class
                           ? val.toString() : type.cast(val));
            }
        }

        // this happens when all values are null
        if (type == null)
            return null;

        Object merged = Array.newInstance(type, unique.size());
        int count = 0;
        for (Object val : values) {
            if (val == null)
                ;
            else if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(val, i);
                    if (v != null) {
                        v = type == String.class ? v.toString() : type.cast(v);
                        if (unique.remove(v))
                            Array.set(merged, count++, v);
                    }
                }
            }
            else {
                val = type == String.class ? val.toString() : type.cast(val);
                if (unique.remove(val)) {
                    Array.set(merged, count++, val);
                }
            }
        }
        return merged;
        //return unique.toArray(empty);
    }

    static public boolean equals (Object u, Object v) {
        if (u == v) return true;
        if ((u != null && v == null) || (u == null && v != null))
            return false;
        Set uset = new HashSet ();
        if (u.getClass().isArray()) {
            int len = Array.getLength(u);
            if (len == 0)
                return false;
            
            for (int i = 0; i < len; ++i)
                uset.add(Array.get(u, i));
        }
        else
            uset.add(u);

        if (v.getClass().isArray()) {
            int len = Array.getLength(v);
            if (len == 0 || len != uset.size())
                return false;
            
            for (int i = 0; i < len; ++i)
                if (!uset.remove(Array.get(v, i)))
                    return false;
        }
        else if (!uset.remove(v))
            return false;
        
        return uset.isEmpty();
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

    static Molecule parseChemical (JsonNode node) throws Exception {
        JsonNode struc = node.get("structure");
        if (struc == null) {
            logger.warning("No \"structure\" found in json!");
            return null;
        }
        
        JsonNode molfile = struc.get("molfile");
        if (molfile == null) {
            logger.warning("No \"molfile\" found in \"structure\"!");
            return null;
        }
            
        MolHandler mh = new MolHandler (molfile.asText());
        Molecule mol = mh.getMolecule();
                    
        JsonNode unii = node.get("approvalID");
        if (unii == null) {
            unii = node.get("UNII");
        }

        if (unii != null) {
            mol.setProperty("UNII", unii.asText());
            mol.setName(unii.asText());
        }
        else {
            logger.warning("No UNII found!");
        }
        
        Map<String, Object> props = parseSubstance (node);
        for (Map.Entry<String, Object> me : props.entrySet()) {
            final StringBuilder sb = new StringBuilder ();
            Stream.of(toArray(me.getValue())).forEach(v -> sb.append(v+"\n"));
            mol.setProperty(me.getKey(), sb.toString());
        }
        return mol;
    }

    static Map<String, Object> parseSubstance (JsonNode node) {
        Map<String, Object> map = new TreeMap<>();
        JsonNode unii = node.get("approvalID");
        if (unii == null) {
            unii = node.get("UNII");
        }

        if (unii != null) {
            map.put("UNII", unii.asText());
        }
        else {
            logger.warning("No UNII found!");
        }

        JsonNode cls = node.get("substanceClass");
        if (cls != null) {
            map.put("Class", cls.asText());
        }
        if (node.has("uuid"))
            map.put("uuid", node.get("uuid").asText());
                    
        JsonNode names = node.get("names");
        if (names != null && names.isArray()) {
            int size = names.size();
            StringBuilder sb = new StringBuilder ();
            for (int i = 0; i < size; ++i) {
                JsonNode n = names.get(i);
                String type = n.get("type").asText();
                String name = n.get("name").asText();
                // make sure the synonym must be more than 3 characters!
                switch (type) {
                case "cn":
                    if (name.length() > 3) {
                        Object val = map.get("Synonyms");
                        map.put("Synonyms", val != null
                                ? Util.merge(val, name) : name);
                        if (n.get("displayName").asBoolean()) {
                            map.put("PreferredName", name);
                        }
                    }
                    break;
                case "cd":
                    { Object val = map.get("Codes");
                        map.put("Codes", val != null ? Util.merge(val, name)
                                : name);
                    }
                    break;
                }
            }
        }
                    
        JsonNode codes = node.get("codes");
        if (codes != null && codes.isArray()) {
            int size = codes.size();
            for (int i = 0; i < size; ++i) {
                JsonNode n = codes.get(i);
                if (n.has("type")
                    && "PRIMARY".equals(n.get("type").asText())) {
                    String sys = n.get("codeSystem").asText();
                    Object val = map.get(sys);
                    String code = n.get("code").asText();
                    map.put(sys, val != null ? Util.merge(val, code) : code);
                    switch (sys) {
                    case "NCI_THESAURUS":
                        code = "NCIT:"+code;
                        break;
                    case "PUBCHEM":
                        code = "CID:"+code;
                        break;
                    case "MESH":
                    case "CAS":
                        code = sys+":"+code;
                        break;
                    case "ChEMBL":
                        break;
                    default:
                        code = null;
                    }
                    if (code != null) {
                        val = map.get("Codes");
                        map.put("Codes", val != null ? Util.merge(val, code) : code);
                    }
                }
            }
        }

        JsonNode refs = node.get("references");
        if (refs != null && refs.isArray()) {
            int size = refs.size();
            StringBuilder sb = new StringBuilder ();
            for (int i = 0; i < size; ++i) {
                JsonNode n = refs.get(i);
                if (n.has("citation")) {
                    String cite = n.get("citation").asText();
                    Object val = map.get("References");
                    if (val != null)
                        val = Util.merge(val, cite);
                    else
                        val = cite;
                    map.put("References", val);
                }
            }
        }

        JsonNode rels = node.get("relationships");
        if (rels != null && rels.isArray()) {
            Set<String> activeMoieties = new TreeSet<>();
            for (int i = 0; i < rels.size(); ++i) {
                JsonNode n = rels.get(i);
                String type = n.get("type").asText();
                String id = n.get("relatedSubstance").get("approvalID").asText();
                Object val = map.get("relationships");
                if (val != null)
                    val = Util.merge(val, type+"|"+id);
                else
                    val = type+"|"+id;
                if ("ACTIVE MOIETY".equals(type))
                    activeMoieties.add(id);
                map.put("relationships", val);
            }
            map.put("ActiveMoieties", activeMoieties.toArray(new String[0]));
        }

        return map;
    }
    
    /*
     * parse SRS json into a Molecule
     */
    public static Object fromJson (String json) {
        Object retobj = null;
        try {
            ObjectMapper mapper = new ObjectMapper ();      
            JsonNode node = mapper.readTree(json);
            String cls = node.get("substanceClass").asText();
            switch (cls) {
            case "chemical":
                retobj = parseChemical (node);
                break;

            default:
                retobj = parseSubstance (node);
            }
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't parse json", ex);
        }
        return retobj;
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

    public static ObjectNode toJsonNode (PropertyContainer container) {
        ObjectMapper mapper = new ObjectMapper ();
        return toJsonNode (mapper, mapper.createObjectNode(), container);
    }

    public static ObjectNode toJsonNode (ObjectNode node,
                                         PropertyContainer container) {
        return toJsonNode (new ObjectMapper (), node, container);
    }
    
    public static ObjectNode toJsonNode (ObjectMapper mapper, ObjectNode node,
                                         PropertyContainer container) {
        for (Map.Entry<String, Object> me :
                 container.getAllProperties().entrySet()) {
            Object value = me.getValue();
            node.put(me.getKey(), mapper.valueToTree(value));
        }
        return node;
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

    public static boolean contains (Object value, Predicate pred) {
        if (value == null) return false;
        if (value.getClass().isArray()) {
            int len = Array.getLength(value);
            for (int i = 0; i < len; ++i) {
                if (pred.test(Array.get(value, i)))
                    return true;
            }
            return false;
        }
        return pred.test(value);
    }

    public static boolean isTetrahedral (MolAtom atom, int[] gi) {
        Molecule m = (Molecule)atom.getParent();
        // check for symmetry 
        Set<Integer> g = new HashSet<Integer>();
        for (int i = 0; i < atom.getBondCount(); ++i) {
            MolBond b = atom.getBond(i);
            if (b.getType() != 1)
                return false;

            MolAtom xa = b.getOtherAtom(atom);
            // any symmetry is false
            if (!g.add(gi[m.indexOf(xa)]))
                return false;
        }

        return g.size() > 2 
            && atom.getAtno() != 7 
            && atom.getAtno() != 15;
    }

    public static boolean isQuatAmine (MolAtom atom) {
        return atom.getAtno() == 7 && atom.getBondCount() == 4;
    }
    
    public static Map calcMolProps (Molecule mol) {
        if (mol.getDim() < 2) {
            mol.clean(2, null);
        }

        // no explicit Hs
        mol.hydrogenize(false);
        // make sure molecule is kekulized consistently
        mol.aromatize();
        mol.dearomatize();

        MolAtom[] atoms = mol.getAtomArray();
        int stereo = 0, def = 0, charge = 0;
        
        int[] gi = new int[atoms.length];
        mol.getGrinv(gi);
        
        for (int i = 0; i < atoms.length; ++i) {
            int chiral = mol.getChirality(i);
            MolAtom atom = mol.getAtom(i);
            
            if (chiral == MolAtom.CHIRALITY_R
                || chiral == MolAtom.CHIRALITY_S) {
                ++def;
                ++stereo;
                atom.setAtomMap(chiral == MolAtom.CHIRALITY_R ? 1 : 2);
            }
            else {
                boolean undef = chiral != 0;
                
                int pc = 0;
                // now check to see if for bond parity.. if any is 
                // defined then we consider this stereocenter as
                // defined!
                for (int k = 0; k < atom.getBondCount(); ++k) {
                    MolBond bond = atom.getBond(k);
                    int parity = bond.getFlags() & MolBond.STEREO1_MASK;
                    if ((parity == MolBond.UP || parity == MolBond.DOWN)
                        && bond.getAtom1() == atom)
                        ++pc;
                }
                
                boolean tetra = isTetrahedral (atom, gi);
                if (isQuatAmine (atom) 
                    || (pc > 0 && atom.getBondCount() > 2 )
                    || (undef && tetra)) {
                    ++stereo;
                    
                    if (pc > 0) {
                        ++def;
                        atom.setAtomMap(0);
                    }
                    else
                        atom.setAtomMap(3); // unknown
                }
            }
            
            charge += atoms[i].getCharge();
        }
        
        int ez = 0; // ez centers
        MolBond[] bonds = mol.getBondArray();
        for (int i = 0; i < bonds.length; ++i) {
            MolBond bond = bonds[i];
            if (mol.isRingBond(i)) {
            }
            else if (bond.getType() == 2) {
                MolAtom a1 = bond.getAtom1();
                MolAtom a2 = bond.getAtom2();
                if (a1.getBondCount() == 1 || a2.getBondCount() == 1) {
                    // nothing to do
                }
                else {
                    /*
                     *  \      /
                     *   \    /
                     *    ----
                     * a1 ---- a2
                     *   /         \
                     *  /          \
                     *
                     */
                    int g1 = -1;
                    for (int j = 0; j < a1.getBondCount(); ++j) {
                        MolBond b = a1.getBond(j);
                        if (b != bond) {
                            int index = mol.indexOf(b.getOtherAtom(a1));
                            if (gi[index] == g1) {
                                g1 = -1;
                                break;
                            }
                            g1 = gi[index];
                        }
                    }
                    
                    int g2 = -1;
                    for (int j = 0; j < a2.getBondCount(); ++j) {
                        MolBond b = a2.getBond(j);
                        if (b != bond) {
                            int index = mol.indexOf(b.getOtherAtom(a2));
                            if (gi[index] == g2) {
                                g2 = -1;
                                break;
                            }
                            g2 = gi[index];
                        }
                    }
                    
                    if (g1 >= 0 && g2 >= 0)
                        ++ez;
                }
            }
        }

        Map props = new HashMap ();
        props.put("undefinedStereo", stereo - def);
        props.put("definedStereo", def);
        props.put("stereoCenters", stereo);
        props.put("ezCenters", ez);
        props.put("charge", charge);
        props.put("formula", mol.getFormula());
        props.put("mwt", mol.getMass());
        props.put("molfile", mol.toFormat("mol"));

        return props;
    }

    public static Molecule getMol (String molfile) {
        try {
            MolHandler mh = new MolHandler (molfile);
            Molecule mol = mh.getMolecule();
            if (mol.getDim() < 2)
                mol.clean(2, null);
            return mol;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            logger.warning("Not a valid molfile: "+molfile);
        }
        return null;
    }

    public static boolean isGroup1Metal (Molecule mol) {
        for (MolAtom a : mol.getAtomArray()) {
            switch (a.getAtno()) {
            case 3: // Li
            case 11: // Na
            case 19: // K
            case 37: // Rb
            case 55: // Cs
            case 87: // Fr
                return true;
            }
        }
        return false;
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
                                         clique.potential()));
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
            if (val == null)
                ;
            else if (val.getClass().isArray()) {
                int len = Array.getLength(val);
                for (int i = 0; i < len; ++i) {
                    if (i > 0) ps.print(",");
                    ps.print(" \""+Array.get(val, i)+"\"");
                }
            }
            else
                ps.print(" "+val);
            ps.println();
        }
        ps.println();
    }

    public static <T extends org.neo4j.graphdb.Entity>
        void index (Index<T> index, T entity, String key, Object value) {
        if (value.getClass().isArray()) {
            try {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(value, i);
                    if ((v instanceof String) && ((String)v).length() > 4000)
                        continue;
                    index.add(entity, key, v);
                }
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE,
                           "Can't add index for entity "
                           +entity.getId()+": key="+key
                           +" value="+value, ex);
            }
        }
        else if ((value instanceof String) && ((String)value).length() > 4000)
            ;
        else
            index.add(entity, key, value);
    }

    public static <T extends org.neo4j.graphdb.Entity>
        void index (Index<T> index, T entity, PropertyContainer props) {
        for (Map.Entry<String, Object> me
                 : props.getAllProperties().entrySet())
            Util.index(index, entity, me.getKey(), me.getValue());
    }

    public static boolean setJson (ObjectNode node, String name, Object value) {
        if (value == null) return false;
        if (value instanceof String[]) {
            ArrayNode a = node.arrayNode();
            for (int i = 0; i < Array.getLength(value); ++i) {
                a.add((String)Array.get(value, i));
            }
            node.put(name, a);
        }
        else if (value instanceof int[]) {
            ArrayNode a = node.arrayNode();
            for (int i = 0; i < Array.getLength(value); ++i) {
                a.add((Integer)Array.get(value, i));
            }
            node.put(name, a);
        }
        else if (value instanceof double[]) {
            ArrayNode a = node.arrayNode();
            for (int i = 0; i < Array.getLength(value); ++i) {
                a.add((Double)Array.get(value, i));
            }
            node.put(name, a);      
        }
        else if (value instanceof String) {
            node.put(name, (String)value);
        }
        else if (value instanceof Integer) {
            node.put(name, (Integer)value);
        }
        else if (value instanceof Double) {
            node.put(name, (Double)value);
        }
        else if (value instanceof Boolean) {
            node.put(name, (Boolean)value);
        }
        else {
            logger.warning("Don't know how to set json value: "
                           +value+" ["+value.getClass()+"]");
        }
        return true;
    }
}
