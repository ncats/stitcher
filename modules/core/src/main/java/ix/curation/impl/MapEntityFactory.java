package ix.curation.impl;

import java.net.URL;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;

import java.util.EnumMap;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.Callable;

import ix.curation.*;

public class MapEntityFactory extends EntityRegistry<Map<String, Object>> {
    static final Logger logger =
        Logger.getLogger(MapEntityFactory.class.getName());

    private String idField = null;

    protected EnumMap<StitchKey, Set<String>> stitches =
        new EnumMap<StitchKey, Set<String>>(StitchKey.class);
    
    public MapEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (GraphDb graphDb) {
        super (graphDb);
    }
    
    public void setId(String idField) {this.idField = idField;}
    public void clear () { stitches.clear(); }    
    public void add (StitchKey key, String property) {
        Set<String> props = stitches.get(key);
        if (props == null) {
            stitches.put(key, props = new TreeSet<String>());
        }
        props.add(property);
    }
    public Set<String> get (StitchKey key) {
        return stitches.get(key);
    }

    @Override
    public Entity register (final Map<String, Object> map) {
        return execute (new Callable<Entity> () {
                public Entity call () throws Exception {
                    return _register (map);
                }
            }, true);
    }

    protected Entity _register (Map<String, Object> map) {
        Entity ent = Entity._getEntity(_createNode (EntityType.Agent));
        String id = null;
        if (idField != null && map.containsKey(idField))
            id = map.get(idField).toString();

        DefaultPayload payload = new DefaultPayload (getDataSource ());
        payload.putAll(map);
        
        for (Map.Entry<StitchKey, Set<String>> me : stitches.entrySet()) {
            for (String prop : me.getValue()) {
                ent._add(me.getKey(), new StitchValue (prop, map.get(prop)));
            }
        }
        ent._add(payload);
        return ent;
    }

    public DataSource register (File file,  String... header)
        throws IOException {
        return register (file, "\t", header);
    }

    @Override
    public DataSource register (File file) throws IOException {
        return register (file, "\t", (String[])null);
    }
    
    public DataSource register (File file, String delim, String... header)
        throws IOException {
        DataSource ds = super.register(file);
        Integer instances = (Integer) ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            int count = register (ds.openStream(), delim, header);
            ds.set("_header", header);      
            ds.set(INSTANCES, count);
        }
        return ds;
    }

    public int register (InputStream is) throws IOException {
        return register (is, "\t", (String[])null);
    }
    
    public int register (InputStream is, String... header) throws IOException {
        return register (is, "\t", header);
    }
    
    public int register (InputStream is, String delim, String... header)
        throws IOException {
        BufferedReader br = new BufferedReader (new InputStreamReader (is));
        int ln = 1;
        for (String line; (line = br.readLine()) != null; ++ln) {
            String[] toks = line.split(delim);
            if (header == null) {
                header = toks;
            }
            else {
                Map<String, Object> row = new HashMap<String, Object>();
                if (header.length != toks.length) {
                    logger.warning(ln + ": mismatch token count");
                }
                int len = Math.min(header.length, toks.length);
                for (int i = 0; i < len; ++i) {
                    String[] values = toks[i].split("\\|");
                    if (values.length > 1) {
                        row.put(header[i], values);
                    }
                    else {
                        row.put(header[i], toks[i]);
                    }
                }
                
                Entity ent = register (row);
            }
        }
        return ln;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+MapEntityFactory.class.getName()
                               +" DBDIR [sourcelabel] FILE PROPS...");
            System.exit(1);
        }
        
        MapEntityFactory mef = new MapEntityFactory (argv[0]);
        for (int i = 3; i < argv.length; ++i) {
            String[] toks = argv[i].split(":");
            if (toks.length == 2) {
                if ("id".equalsIgnoreCase(toks[0])) {
                    mef.setId(toks[1]);
                    logger.info("id property: "+toks[1]);
                }
                else {
                    StitchKey key = StitchKey.valueOf(toks[0]);
                    mef.add(key, toks[1]);
                    logger.info(key + " => \"" + toks[1] + "\"");
                }
            }
        }

        try {
            if (argv[2].startsWith("http")) {
                DataSource ds = mef.register(new URL (argv[2]));
                ds.setName(argv[1]);
            }
            else {
                File file = new File (argv[2]);
                if (file.exists()) {
                    DataSource ds = mef.register(file);
                    ds.setName(argv[1]);
                }
                else {
                    logger.log(Level.SEVERE, "File "+file+" doesn't exist!");
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }    
}
