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

public class MapEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(MapEntityFactory.class.getName());

    public MapEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (GraphDb graphDb) {
        super (graphDb);
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
        int ln = 1, count = 0;
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
                if (ent != null)
                    ++count;
            }
        }
        return count;
    }

    public static void main (String[] argv) throws Exception {
        register (argv, MapEntityFactory.class);
    }
}
