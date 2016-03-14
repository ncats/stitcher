package ix.curation;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeSupport;
import java.beans.PropertyChangeListener;

import java.net.URL;
import java.net.URI;
import java.io.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.Callable;
import java.lang.reflect.Constructor;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;

public class EntityRegistry extends EntityFactory {
    static final Logger logger =
        Logger.getLogger(EntityRegistry.class.getName());

    protected final PropertyChangeSupport pcs =
        new PropertyChangeSupport (this);

    protected DataSource source;
    protected DataSourceFactory dsf;
    protected String idField;
    protected String strucField;
    
    protected EnumMap<StitchKey, Set<String>> stitches;
    protected Map<String, StitchKeyMapper> mappers;
    // stitch key due to mappers
    protected EnumMap<StitchKey, Set<String>> stitchMappers;
    
    public EntityRegistry (String dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }
    public EntityRegistry (File dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }
    public EntityRegistry (GraphDatabaseService gdb) {
        this (GraphDb.getInstance(gdb));
    }
    
    public EntityRegistry (GraphDb graphDb) {
        super (graphDb);
        dsf = new DataSourceFactory (graphDb);
        init ();
    }

    // to be overriden by subclass
    protected void init () {
        stitches = new EnumMap<StitchKey, Set<String>>(StitchKey.class);
        mappers = new HashMap<String, StitchKeyMapper>();
        stitchMappers = new EnumMap<>(StitchKey.class);
    }

    public EntityRegistry add (String property, StitchKeyMapper mapper) {
        mappers.put(property, mapper);
        return this;
    }
    
    public EntityRegistry setId (String idField) {
        this.idField = idField;
        return this;
    }
    public void clear () { stitches.clear(); }    
    public EntityRegistry add (StitchKey key, String property) {
        Set<String> props = stitches.get(key);
        if (props == null) {
            stitches.put(key, props = new TreeSet<String>());
        }
        props.add(property);
        return this;
    }
    public Set<String> get (StitchKey key) {
        return stitches.get(key);
    }

    public DataSource registerFromConfig (String config) throws Exception {
        return registerFromConfig (new File (config));
    }
    
    public DataSource registerFromConfig (File config) throws Exception {
        return registerFromConfig (config.getParentFile(),
                                   ConfigFactory.parseFile(config));
    }

    public DataSource registerFromConfig (InputStream is) throws Exception {
        return registerFromConfig (new File ("."), is);
    }

    public DataSource registerFromConfig (File base, InputStream is)
        throws Exception {
        return registerFromConfig
            (base, ConfigFactory.parseReader(new InputStreamReader (is)));
    }
        
    protected DataSource registerFromConfig (File base, Config conf)
        throws Exception {
        
        if (!conf.hasPath("source")) {
            throw new IllegalArgumentException
                ("Configuration contains no \"source\" definition!");
        }
        
        Config source = conf.getConfig("source");
        if (!source.hasPath("data"))
            throw new IllegalArgumentException
                ("Source has no required \"data\" element defined!");
        
        //System.out.println("source:"+source);
        if (conf.hasPath("cache")) {
            String cache = conf.getString("cache");
            graphDb.setCache(CacheFactory.getInstance(cache));
        }

        idField = null;
        if (source.hasPath("idf"))
            setId (source.getString("idf"));

        strucField = null;
        if (source.hasPath("structure")) {
            strucField = source.getString("structure");
        }

        if (conf.hasPath("stitches")) {
            List<? extends ConfigObject> list = conf.getObjectList("stitches");
            RegexStitchKeyMapper mapper = new RegexStitchKeyMapper ();
            
            for (ConfigObject obj : list) {
                Config cf = obj.toConfig();
                if (!cf.hasPath("key")) {
                    logger.warning
                        ("Stitch element doesn't have \"key\" defined!");
                    continue;
                }

                if (!cf.hasPath("property")) {
                    logger.warning
                        ("Stitch element doesn't have \"property\" defined!");
                    continue;
                }

                StitchKey key;
                try {
                    key = StitchKey.valueOf(cf.getString("key"));
                }
                catch (Exception ex) {
                    logger.warning("Invalid StitchKey value: "
                                   +cf.getString("key"));
                    continue;
                }

                String property = cf.getString("property");
                System.out.println("key="+key+" property=\""+property+"\"");

                if (cf.hasPath("regex")) {
                    ConfigValue cv = cf.getValue("regex");
                    if (cv.valueType() == ConfigValueType.LIST) {
                        List<String> regexes = (List<String>)cv.unwrapped();
                        for (String regex : regexes) {
                            try {
                                mapper.add(key, regex);
                                add (property, mapper);
                            }
                            catch (Exception ex) {
                                logger.warning
                                    ("Bogus stitch regex for "+key+": "
                                     +regex+" ("+ex.getMessage()+")");
                            }
                        }
                    }
                    else {
                        String regex = (String)cv.unwrapped();
                        try {
                            mapper.add(key, regex);
                            add (property, mapper);
                        }
                        catch (Exception ex) {
                            logger.warning
                                ("Bogus stitch regex for "+key+": "
                                 +regex+" ("+ex.getMessage()+")");
                        }
                    }
                }
                else { // treat this as normal stitch key
                    add (key, property);
                }
            }
        }
        else {
            logger.warning("No \"stitches\" value defined!");
        }

        DataSource ds;
        
        String data = source.getString("data");
        if (data.startsWith("http")) {
            ds = register (new URL (data));
        }
        else if (data.startsWith("file")) {
            ds = register (new File (new URI (data)));
        }
        else {
            File file = new File (base, data);
            if (!file.exists()) {
                // now let's try current
                file = new File (data);
                if (!file.exists())
                    throw new IllegalArgumentException
                        ("Can't find data: \""+data+"\"");
            }
            ds = register (file);
        }
        
        if (ds != null && source.hasPath("name"))
            ds.setName(source.getString("name"));
        
        return ds;
    }
    
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
    
    public void addPropertyChangeListener (PropertyChangeListener l) {
        pcs.addPropertyChangeListener(l);
    }
    
    public void removePropertyChangeListener (PropertyChangeListener l) {
        pcs.removePropertyChangeListener(l);
    }

    protected void firePropertyChange (String property,
                                       Object oldVal, Object newVal) {
        pcs.firePropertyChange(property, oldVal, newVal);
    }
    
    protected Node _createNode (EntityType type) {
        if (source == null) {
            throw new IllegalStateException
                ("Can't create entity without a data source!");
        }
        
        Node node = gdb.createNode(type, AuxNodeType.ENTITY,
                                   DynamicLabel.label(source.getKey()));
        node.setProperty(SOURCE, source.getKey());
        return node;
    }
    
    protected Node createNode (EntityType type) {
        try (Transaction tx = gdb.beginTx()) {
            Node node = _createNode (type);
            tx.success();
            return node;
        }
    }
    
    protected void updateMeta (DataSource ds) {
        StitchKey[] keys = stitchMappers.keySet().toArray(new StitchKey[0]);
        String[] sk = new String[keys.length];
        for (int i = 0; i < sk.length; ++i)
            sk[i] = keys[i].name();
        ds.set(STITCHES, sk, true);
        for (Map.Entry<StitchKey, Set<String>> me : stitchMappers.entrySet()) {
            ds.set(me.getKey().name(), me.getValue().toArray(new String[0]));
        }
    }
    
    public DataSourceFactory getDataSourceFactory () { return dsf; }
    public EntityRegistry setDataSource (DataSource source) {
        this.source = source;
        return this;
    }
    public DataSource getDataSource () { return source; }

    public DataSource register (File file) throws IOException {
        return source = getDataSourceFactory().register(file);
    }

    public DataSource register (URL url) throws IOException {
        return source = getDataSourceFactory().register(url);
    }
    
    public static <T extends EntityRegistry>
        void register (String[] argv, Class<T> cls)
        throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: " + cls.getName()
                    + " DBDIR CONFIGS...");
            System.exit(1);
        }

        Constructor<T> inst = cls.getConstructor(String.class);
        T registry = inst.newInstance(argv[0]);
        try {
            for (int i = 1; i < argv.length; ++i) {
                DataSource ds = registry.registerFromConfig(argv[i]);
                System.out.println
                    (ds+": "+ds.get(INSTANCES)+" entities registered!");
            }
        }
        finally {
            registry.shutdown();
        }
    }
}
