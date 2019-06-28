package ncats.stitcher.impl;

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

import com.typesafe.config.Config;
import ncats.stitcher.*;

public class MapEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(MapEntityFactory.class.getName());

    protected String delimiter = "\t";
    protected String[] header;

    public MapEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public MapEntityFactory (GraphDb graphDb) {
        super (graphDb);
    }
    
    public DataSource register (String name, File file,  String... header)
        throws IOException {
        return register (name, file, delimiter, header);
    }

    @Override
    public DataSource register (String name, File file) throws IOException {
        return register (name, file, delimiter, (String[])null);
    }

    @Override
    protected void parseConfig (Config conf) throws Exception {
        super.parseConfig(conf);
        if (conf.getConfig("source").hasPath("delimiter")) {
            delimiter = conf.getConfig("source").getString("delimiter");
            logger.info("### Delimiter: '"+delimiter+"'");
        }
    }
    
    public DataSource register (String name, File file, String delim, String... header)
        throws IOException {
        DataSource ds = super.register(name, file);
        Integer instances = (Integer) ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            instances = register (ds.openStream(), delim, header);
            ds.set(PROPERTIES, header == null ? this.header : header);
            ds.set(INSTANCES, instances);
            updateMeta (ds);
            logger.info("$$$ "+instances+" entities registered for "+ds);
        }
        return ds;
    }

    public int register (InputStream is) throws IOException {
        return register (is, delimiter, (String[])null);
    }
    
    public int register (InputStream is, String... header) throws IOException {
        return register (is, delimiter, header);
    }
    
    public int register (InputStream is, String delim, String... header)
        throws IOException {
        LineTokenizer tokenizer = new LineTokenizer (delim.charAt(0));
        tokenizer.setInputStream(is);
        
        int count = 0;
        while (tokenizer.hasNext()) {
            String[] toks = tokenizer.next();

            if (header == null) {
                this.header = header = toks;
                logger.info("## HEADER: ");
                for (int i = 0; i < header.length; ++i)
                    logger.info("  "+i+": \""+header[i]+"\"");
            }
            else if (header.length != toks.length) {
                logger.warning(tokenizer.getLineCount()
                               + ": mismatch token count; expecting "
                               +header.length+" but got "+toks.length+"!");
            }
            else {
                Map<String, Object> row = new HashMap<String, Object>();
                for (int i = 0; i < toks.length; ++i) {
                    if (toks[i] != null) {
                        String[] values = toks[i].split("\\|");
                        if (values.length > 1) {
                            row.put(header[i], values);
                        }
                        else {
                            row.put(header[i], toks[i]);
                        }
                    }
                }

                if (!row.isEmpty()) {
                    Entity ent = register (row);
                    if (ent != null)
                        ++count;
                }
            }
        }
        return count;
    }

    public static void main (String[] argv) throws Exception {
        register (argv, MapEntityFactory.class);
    }
}
