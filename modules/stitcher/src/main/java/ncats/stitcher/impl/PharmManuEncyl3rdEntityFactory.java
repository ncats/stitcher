package ncats.stitcher.impl;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.*;
import java.util.*;
import java.util.regex.*;

import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;
import ncats.stitcher.Entity;
import ncats.stitcher.DataSource;
import ncats.stitcher.EntityRegistry;
import static ncats.stitcher.StitchKey.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import ncats.stitcher.calculators.events.PharmManuEventParser;

public class PharmManuEncyl3rdEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(RanchoJsonEntityFactory.class.getName());

    int count;
    Base64.Encoder base64 = Base64.getEncoder();
    ObjectMapper mapper = new ObjectMapper ();
    Set<String> props = new TreeSet<>();
    Pattern casregex = Pattern.compile("([0-9^\\-]+)-([0-9]{2})-([0-9]{1})");

    public PharmManuEncyl3rdEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public PharmManuEncyl3rdEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public PharmManuEncyl3rdEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("UNII");
        setNameField ("Drug Substance");
        setEventParser(PharmManuEventParser.class.getCanonicalName());
        add (I_UNII, "UNII");
    }
    
    public int register (InputStream is) throws IOException {
        JsonNode root = mapper.readTree(is);
        count = 0;
        int total = root.size();
        for (int i = 0; i < total; ++i) {
            JsonNode node = root.get(i);
            try {
                register (node, total);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "Can't register node "
                           +node.get("Drug Substance").asText(), ex);
            }
        }
        return count;
    }

    
    void set (Map<String, Object> map, String name, JsonNode node)
        throws Exception {
        Object value = map.get(name);
        if (node.isObject()) {
            value = value != null
                ? Util.merge(value, base64.encodeToString
                             (mapper.writeValueAsBytes(node)))
                : base64.encodeToString(mapper.writeValueAsBytes(node));
            map.put(name, value);
        }
        else if (node.isArray()) {
            StringBuilder buf = new StringBuilder ();
            for (int i = 0; i < node.size(); ++i) {
                JsonNode n = node.get(i);
                String val;
                if (n.isObject()) {
                    val = base64.encodeToString
                        (mapper.writeValueAsBytes(n));
                }
                else
                    val = n.asText();
                
                if (val.length() > 3) {
                    if (buf.length() > 0) buf.append("\n");
                    buf.append(val);
                }
            }

            value = value != null ? Util.merge(value, buf.toString())
                : buf.toString();
            map.put(name, value);
        }
        else {
            String text = node.asText();
            if (text.length() > 3) {
                if (name.equals("UNII")
                    && ("NOT FOUND".equalsIgnoreCase(text)
                        || "MIXTURE".equalsIgnoreCase(text))) {
                    return;
                }
                else if (name.equals("Common Name")) {
                    for (String n : text.split(";")) {
                        value = value != null
                            ? Util.merge(value, n.trim()) : n.trim();
                    }
                }
                else if (name.equals("CAS")) {
                    Matcher m = casregex.matcher(text);
                    while (m.find()) {
                        String cas = m.group(1) + "-"+m.group(2)+"-"+m.group(3);
                        value = value != null ? Util.merge(value, cas) : cas;
                    }
                }
                else {
                    value = value != null ? Util.merge(value, text) : text;
                }
                map.put(name, value);           
            }
        }
    }
    
    void register (JsonNode node, int total) throws Exception {
        System.out.println("+pme+ "+(count+1)+"/"+total+" +++++");
        Map<String, Object> map = new TreeMap<>();
        List<String> products = new ArrayList<>();      
        for (Iterator<Map.Entry<String, JsonNode>> it = node.fields();
             it.hasNext(); ) {
            Map.Entry<String, JsonNode> f = it.next();
            String name = f.getKey();
            JsonNode n = f.getValue();
            set (map, name, n);
            if (name.equals("Drug Products")) {
                for (int i = 0; i < n.size(); ++i) {
                    JsonNode p = n.get(i);
                    if (p.has("Product")) {
                        String pn = p.get("Product").asText();
                        if (pn.length() > 3)
                            products.add(pn);
                    }
                }
            }
        }
        props.addAll(map.keySet());

        /*
        if (!products.isEmpty())
            map.put("Products", products.toArray(new String[0]));
        */
        
        Entity ent = register (map);
        ++count;
    }

    @Override
    public DataSource register (String name, File file) throws IOException {
        DataSource ds = super.register(name, file);
        Integer instances = (Integer) ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            instances = register (ds.openStream());
            if (!props.isEmpty())
                ds.set(PROPERTIES, props.toArray(new String[0]));
            ds.set(INSTANCES, instances);
            updateMeta (ds);
            logger.info("$$$ "+instances+" entities registered for "+ds);
        }
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory DB FILE");
            System.exit(1);
        }
        
        PharmManuEncyl3rdEntityFactory mef = new PharmManuEncyl3rdEntityFactory (argv[0]);
        String sourceName = null;
        try {
            for (int i = 1; i < argv.length; ++i) {
                int pos = argv[i].indexOf('=');
                if (pos > 0) {
                    String name = argv[i].substring(0, pos);
                    if (name.equalsIgnoreCase("cache")) {
                        mef.setCache(argv[i].substring(pos+1));
                    }
                    else if (name.equalsIgnoreCase("name")) {
                        sourceName = argv[i].substring(pos+1);
                        System.out.println(sourceName);
                    }
                    else {
                        logger.warning("** Unknown parameter \""+name+"\"!");
                    }
                }
                else {
                    File file = new File(argv[i]);

                    if(sourceName != null){
                        mef.register(sourceName, file);
                    }
                    else {
                        mef.register(argv[i], file);
                    }
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }
}
