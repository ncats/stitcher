package ncats.stitcher.impl;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.*;
import java.util.*;

import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;
import ncats.stitcher.Entity;
import static ncats.stitcher.StitchKey.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class PharmManuEncyl3rdEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(RanchoJsonEntityFactory.class.getName());

    int count;
    Base64.Encoder base64 = Base64.getEncoder();
    ObjectMapper mapper = new ObjectMapper ();

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
        add (N_Name, "Common Name")
            .add (I_CAS, "CAS")
            .add (I_UNII, "UNII")
            .add (N_Name, "Products")
            ;
    }
    
    @Override
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
                if (buf.length() > 0) buf.append("\n");
                buf.append(val);
            }

            value = value != null ? Util.merge(value, buf.toString())
                : buf.toString();
            map.put(name, value);
        }
        else {
            value = value != null ? Util.merge(value, node.asText())
                : node.asText();
            map.put(name, value);
        }
    }
    
    void register (JsonNode node, int total) throws Exception {
        System.out.println("+++++ "+(count+1)+"/"+total+" +++++");
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
                    JsonNode p = node.get(i);
                    if (p.has("Product"))
                        products.add(p.get("Product").asText());
                }
            }
        }

        if (!products.isEmpty())
            map.put("Products", products.toArray(new String[0]));
        
        Entity ent = register (map);
        ++count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ncats.stitcher.impl.PharmManuEncyl3rdEntityFactory DB FILE");
            System.exit(1);
        }
        
        PharmManuEncyl3rdEntityFactory pharm =
            new PharmManuEncyl3rdEntityFactory (argv[0]);
        logger.info("***** registering "+argv[1]+" ******");
        pharm.register(new File (argv[1]));
        pharm.shutdown();
    }
}
