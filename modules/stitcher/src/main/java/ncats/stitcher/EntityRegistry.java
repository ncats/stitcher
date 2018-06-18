package ncats.stitcher;

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
import java.lang.reflect.Array;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

import lychi.LyChIStandardizer;
import lychi.SaltIdentifier;
import lychi.ElementData;
import lychi.util.ChemUtil;

import static ncats.stitcher.StitchKey.*;

public class EntityRegistry extends EntityFactory {
    static final Logger logger =
        Logger.getLogger(EntityRegistry.class.getName());

    static class Reference {
        public DataSource ds;
        public StitchKey key;
        public String id;
    }

    class SV extends StitchValue {
        public SV (Object value) {
            super (value);
        }
        public SV (String name, Object value) {
            super (name, value);
        }
        public SV (StitchKey key, Object value) {
            super (key, value);
        }

        @Override
        public boolean isBlacklist (StitchKey key) {
            return EntityRegistry.this.isBlacklist(key, value);
        }
        
        @Override
        public boolean isBlacklist () {
            return key != null ? EntityRegistry.this.isBlacklist(key, value)
                : EntityRegistry.this.isBlacklist(value);
        }
    }

    protected final PropertyChangeSupport pcs =
        new PropertyChangeSupport (this);

    protected DataSource source;
    protected String idField; // id field
    protected String nameField; // preferred name field
    protected String strucField;
    
    protected EnumMap<StitchKey, Set<String>> stitches;
    protected Map<String, StitchKeyMapper> mappers;
    // stitch key due to mappers
    protected EnumMap<StitchKey, Set<String>> stitchMappers;
    protected List<Reference> references = new ArrayList<>();
    protected Map<StitchKey, Set<Object>> blacklist;
    
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
        init ();
    }

    // to be overriden by subclass
    protected void init () {
        stitches = new EnumMap<StitchKey, Set<String>>(StitchKey.class);
        mappers = new HashMap<String, StitchKeyMapper>();
        stitchMappers = new EnumMap<>(StitchKey.class);
        blacklist = new HashMap<>();
    }

    public void addBlacklist (Object... values) {
        for (Object v : values)
            for (StitchKey key : Entity.KEYS)
                addBlacklist (key, v);
    }
    
    public void addBlacklist (StitchKey key, Object... values) {
        Set set = blacklist.get(key);
        if (set == null)
            blacklist.put(key, set = new HashSet ());
        for (Object v : values)
            set.add(v);
    }

    public boolean isBlacklist (StitchKey key, Object value) {
        Set set = blacklist.get(key);
        if (set != null)
            return set.contains(value);
        return false;
    }

    public boolean isBlacklist (Object value) {
        for (StitchKey key : Entity.KEYS)
            if (isBlacklist (key, value))
                return true;
        return false;
    }
    
    public void removeBlacklist (StitchKey key) {
        blacklist.remove(key);
    }

    public void removeBlacklist (StitchKey key, Object... values) {
        Set set = blacklist.get(key);
        if (set != null) {
            for (Object v : values)
                set.remove(v);
        }
    }

    public EntityRegistry add (StitchKeyMapper mapper, String property) {
        mappers.put(property, mapper);
        return this;
    }
    
    public EntityRegistry setIdField (String idField) {
        this.idField = idField;
        return this;
    }
    public String getIdField () { return idField; }
    
    public EntityRegistry setNameField (String nameField) {
        this.nameField = nameField;
        return this;
    }
    public String getNameField () { return nameField; }

    public EntityRegistry setStrucField (String strucField) {
        this.strucField = strucField;
        return this;
    }
    public String getStrucField () { return strucField; }
    
    public void clear () { stitches.clear(); }
    
    public EntityRegistry add (StitchKey key, String property) {
        Set<String> props = stitches.get(key);
        if (props == null) {
            stitches.put(key, props = new TreeSet<String>());
        }
        props.add(property);
        return this;
    }
    
    public Set<String> getProperties (StitchKey key) {
        return stitches.get(key);
    }
    
    public StitchKey getStitchKey (String prop) {
        for (Map.Entry<StitchKey, Set<String>> me : stitches.entrySet()) {
            if (me.getValue().contains(prop))
                return me.getKey();
        }
        return null;
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

    protected void parseConfig (Config conf) throws Exception {
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
            setCache (cache);
        }

        idField = null;
        if (source.hasPath("idf"))
            setIdField (source.getString("idf"));

        nameField = null;
        if (source.hasPath("namef"))
            setNameField (source.getString("namef"));

        strucField = null;
        if (source.hasPath("structure")) {
            strucField = source.getString("structure");
        }

        if (conf.hasPath("stitches")) {
            List<? extends ConfigObject> list = conf.getObjectList("stitches");
            
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
                logger.info("key="+key+" property=\""+property+"\"");
                
                if (cf.hasPath("regex")) {
                    RegexStitchKeyMapper mapper = new RegexStitchKeyMapper ();
                    if (cf.hasPath("minlen")) {
                        mapper.setMinLength(cf.getInt("minlen"));
                    }
                    
                    if (cf.hasPath("normalize")) {
                        mapper.setNormalized(cf.getBoolean("normalize"));
                    }

                    if (cf.hasPath("blacklist")) {
                        ConfigValue cv = cf.getValue("blacklist");
                        if (cv.valueType() == ConfigValueType.LIST) {
                            List<String> values = (List<String>)cv.unwrapped();
                            for (String v : values)
                                mapper.addBlacklist(v);
                        }
                        else {
                            mapper.addBlacklist(cv.unwrapped());
                        }
                    }
                    
                    ConfigValue cv = cf.getValue("regex");
                    if (cv.valueType() == ConfigValueType.LIST) {
                        List<String> regexes = (List<String>)cv.unwrapped();
                        for (String regex : regexes) {
                            try {
                                logger.info(key+" derived from regex \""
                                            +regex+"\" on property \""
                                            +property+"\"");
                                mapper.add(key, regex);
                                add (mapper, property);
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
                            logger.info(key+" derived from regex \""
                                        +regex+"\" on property \""
                                        +property+"\"");
                            mapper.add(key, regex);
                            add (mapper, property);
                        }
                        catch (Exception ex) {
                            logger.warning
                                ("Bogus stitch regex for "+key+": "
                                 +regex+" ("+ex.getMessage()+")");
                        }
                    }
                }
                else if (cf.hasPath("blacklist")) {
                    BlacklistStitchKeyMapper mapper =
                        new BlacklistStitchKeyMapper (key);
                    
                    ConfigValue cv = cf.getValue("blacklist");
                    if (cv.valueType() == ConfigValueType.LIST) {
                        List<String> values = (List<String>)cv.unwrapped();
                        for (String v : values)
                            mapper.addBlacklist(v);
                    }
                    else {
                        mapper.addBlacklist(cv.unwrapped());
                    }
                    add (mapper, property);
                }
                else { // treat this as normal stitch key
                    add (key, property);
                }
            }
        } // stitches
        
        // references data sources
        if (conf.hasPath("references")) {
            List<? extends ConfigObject> list =
                conf.getObjectList("references");
            for (ConfigObject obj : list) {
                Config cf = obj.toConfig();
                if (!cf.hasPath("name")) {
                    logger.warning
                        ("Reference doesn't have \"name\" defined!");
                    continue;
                }
                
                if (!cf.hasPath("key")) {
                    logger.warning
                        ("Reference doesn't have \"key\" defined!");
                }

                Reference ref = new Reference ();
                String name = cf.getString("name");
                try {
                    ref.key = StitchKey.valueOf(cf.getString("key"));
                }
                catch (Exception ex) {
                    logger.warning("Invalid StitchKey value: "
                                   +cf.getString("key"));
                    continue;
                }
                
                ref.ds = dsf.getDataSourceByName(name);
                if (ref.ds == null) {
                    logger.warning("Can't locate data source \""+
                                   name+"\"");
                    continue;
                }
                
                ref.id = cf.getString("id");
                if (ref.id == null)
                    ref.id = idField;
                
                references.add(ref);
            }
        } // references

        if (stitches.isEmpty() && mappers.isEmpty() && references.isEmpty()) {
            logger.log(Level.SEVERE, "No stitches or references defined!");
        }
    }
    
    protected DataSource registerFromConfig (File base, Config conf)
        throws Exception {

        parseConfig (conf);

        Config source = conf.getConfig("source");
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

    /**
     * attach payload to an existing entity
     * @param map a map to attach to entity.
     * @return returns an entity with an attached map.
     */
    public Entity attach (final Map<String, Object> map) {
        try (Transaction tx = gdb.beginTx()) {
            Entity e = _attach (map);
            tx.success();
            return e;
        }
    }

    public Entity _attach (final Map<String, Object> map) {
        Entity ent = null;
        int cnt = 0;
        
        Object id = map.get(idField);
        for (Reference ref : references) {
            Object rid = map.get(ref.id);
            if (rid != null) {
                Iterator<Entity> iter = find (ref.key, rid);
                while (iter.hasNext()) {
                    Entity e = iter.next();
                    if (e._is(ref.ds.getName())) {
                        DefaultPayload payload =
                            new DefaultPayload (source, id);
                        payload.putAll(map);
                        e._add(payload);
                        e._addLabel(source.getName());
                        if (ent == null)
                            ent = e;
                        logger.info
                            ("... attaching "+id+" to entity "+e.getId());
                        ++cnt;
                    }
                }
            }
        }
        logger.info(id+" maps to "+cnt+" entities!");
        
        return ent;
    }
    
    public Entity register (final Map<String, Object> map) {
        try (Transaction tx = gdb.beginTx()) {
            Entity ent = _register (map);
            tx.success();
            return ent;
        }
    }

    public Entity registerIfAbsent (final Map<String, Object> map) {
        Entity ent = null;      
        try (Transaction tx = gdb.beginTx()) {
            Object id = map.get(idField);           
            if (id != null) {
                // check if this has been registered before..
                int dups = 0;
                Iterator<Entity> it = find (idField, id);
                while (it.hasNext()) {
                    Entity e = it.next();
                    if (e._is(source.getName())) {
                        if (ent != null)
                            ++dups;
                        ent = e;
                    }
                }
                
                if (dups > 0)
                    logger.warning(idField+"="+id
                                   +" yields "+dups+" matches!");
            }

            if (ent == null) {
                ent = _register (map);
            }
            else {
                ent = null;
                logger.info(id+" is already registered!");
            }
            tx.success();
            
            return ent;
        }
    }

    protected Entity _register (Map<String, Object> map) {
        Entity ent = null;
        
        if (stitches.isEmpty() && mappers.isEmpty()) {
            ent = _attach (map);
        }
        else {      
            ent = Entity._getEntity(_createNode ());
            String id = null;
            if (idField != null && map.containsKey(idField)) {
                Object o =map.get(idField);
                //do null value check incase it's missing!
                if(o !=null){
                    id = map.get(idField).toString();
                }

            }
            
            DefaultPayload payload = new DefaultPayload (getDataSource (), id);
            payload.putAll(map);
            
            if (strucField != null) {
                Object value = map.get(strucField);
                if (value != null) {
                    if (value instanceof Molecule) {
                        Molecule mol = (Molecule)value;
                        if (mol.getAtomCount() > 0)
                            lychify (ent, mol);
                    }
                    else {
                        try {
                            MolHandler mh = new MolHandler (value.toString());
                            Molecule mol = mh.getMolecule();
                            if (mol.getAtomCount() > 0)
                                lychify (ent, mol);
                        }
                        catch (Exception ex) {
                            logger.warning
                                (id+": Can't parse structure: "+value);
                        }
                    }
                }
            }
            
            for (Map.Entry<StitchKey, Set<String>> me : stitches.entrySet()) {
                for (String prop : me.getValue()) {
                    if (!mappers.containsKey(prop)) { // deal with mappers later
                        Object val = map.get(prop);
                        if (val == null) {
                            continue;
                        }

                        switch (me.getKey()) {
                        case I_SID: // sigh.. should check by type instead!
                        case I_CID:
                        case I_PMID:
                            if (val instanceof Long) {
                                ent._add(me.getKey(),
                                         new StitchValue (prop, val));
                            }
                            else if (val.getClass().isArray()) {
                                int len = Array.getLength(val);
                                for (int i = 0; i < len; ++i) {
                                    Object v = Array.get(val, i);
                                    try {
                                        ent._add
                                            (me.getKey(), new StitchValue
                                             (prop, Long.parseLong
                                              (v.toString())));
                                    }
                                    catch (NumberFormatException ex) {
                                        logger.warning("Bogus long value: "+v);
                                    }
                                }
                            }
                            else {
                                try {
                                    ent._add(me.getKey(), new StitchValue
                                             (prop,
                                              Long.parseLong(val.toString())));
                                }
                                catch (NumberFormatException ex) {
                                    logger.warning("Bogus long value: "+val);
                                }
                            }
                            break;
                            
                        case T_Keyword:
                            if (val.getClass().isArray()) {
                                int len = Array.getLength(val);
                                for (int i = 0; i < len; ++i) {
                                    Object t = Array.get(val, i);
                                    ent._addLabel
                                        (Label.label(t.toString()));
                                }
                            }
                            else 
                                ent._addLabel(Label.label(val.toString()));
                            break;
                        default:
                            { Object value  = normalize (me.getKey(), val);
                                ent._add(me.getKey(),
                                         new StitchValue (prop, value));
                            }
                        }
                    }
                }
            }
            mapValues (ent, map);
            
            ent._add(payload);
        }
        
        return ent;
    }

    /*
     * subclass can override this.. 
     */
    protected Object normalize (StitchKey key, Object value) {
        if (value != null) {
            if (value.getClass().isArray()) {
            }
            else {
                /*
                switch (key) {
                case N_Name:
                case I_UNII:
                case H_InChIKey:
                case H_LyChI_L1:
                case H_LyChI_L2:
                case H_LyChI_L3:
                case H_LyChI_L4:
                case H_LyChI_L5:
                case H_SHA1:
                case H_SHA256:
                case H_MD5:
                    value = value.toString().toUpperCase();
                    break;
                }
                */
                
                if (key.type.isAssignableFrom(Number.class)) {
                    try {
                        value = Long.parseLong(value.toString());
                    }
                    catch (NumberFormatException ex) {
                        logger.log(Level.SEVERE, key
                                   +": Can't convert value \""+value
                                   +"\" to long!", ex);
                    }
                }
                else {
                    value = value.toString().toUpperCase();
                }
            }
        }
        return value;
    }

    protected void mapValues (Entity ent, Map<String, Object> values) {
        stitchMappers.putAll(stitches);
        
        for (Map.Entry<String, StitchKeyMapper> me : mappers.entrySet()) {
            Object value = values.get(me.getKey());
            
            if (value != null) {
                Map<StitchKey, Object> mapping = me.getValue().map(value);
                for (Map.Entry<StitchKey, Object> m : mapping.entrySet()) {
                    //logger.info("mapper "+m.getKey()+" => "+m.getValue());
                    Object val = m.getValue();
                    if (val == null) {
                    }
                    else if (m.getKey() == T_Keyword) {
                        if (val.getClass().isArray()) {
                            int len = Array.getLength(val);
                            for (int i = 0; i < len; ++i) {
                                Object t = Array.get(val, i);
                                ent._addLabel(Label.label(t.toString()));
                            }
                        }
                        else
                            ent._addLabel(Label.label(val.toString()));
                    }
                    else {
                        ent._add(m.getKey(),
                                 new StitchValue (me.getKey(),
                                                  normalize (m.getKey(), val)));
                    }
                    
                    Set<String> props = stitchMappers.get(m.getKey());
                    if (props == null) {
                        stitchMappers.put(m.getKey(),
                                          props = new TreeSet<String>());
                    }
                    props.add(me.getKey());
                }
            }
        }
    }

    protected String[] lychify (final Molecule mol, final boolean stripSalt)
        throws Exception {
        String hash = Util.sha1hex("LyChI:" + mol.exportToFormat("smiles"));
        if (!stripSalt)
            hash = Util.sha1hex("LyChISalt:" + mol.exportToFormat("smiles"));

        String[] hk = getCache().getOrElse(hash, new Callable<String[]> () {
                public String[] call () throws Exception {
                    LyChIStandardizer lychi = new LyChIStandardizer ();
                    // only standardize if 
                    if (mol.getAtomCount() < 1024) {
                        boolean salt = stripSalt;
                        if (salt) {
                            /*
                             * if molecule contains metal and is inorganic, 
                             * then we only remove water
                             */
                            if (LyChIStandardizer.containMetals(mol)
                                /*&& LyChIStandardizer.isInorganic(mol)*/) {
                                // remove only water
                                LyChIStandardizer.dehydrate(mol);
                                salt = false;
                            }
                        }
                        
                        lychi.removeSaltOrSolvent(salt);
                        lychi.standardize(mol);
                    }
                    else {
                        logger.warning
                        ("molecule has "+mol.getAtomCount()
                         +" atoms; no standardization performed!");
                    }

                    String[] hk = LyChIStandardizer.hashKeyArray(mol);
                    String[] re = new String[hk.length+1];
                    re[hk.length] = ChemUtil.canonicalSMILES(mol);

                    boolean metal = false;
                    for (int i = 0; i < mol.getAtomCount(); ++i) {
                        if (ElementData.isMetal(mol.getAtom(i).getAtno())) {
                            metal = true;
                            break;
                        }
                    }

                    String kind;  // metal (M), salt/solvent (S), other (N)
                    if (metal) {
                        kind = "M";
                    }
                    else if (SaltIdentifier
                             .getInstance().isSaltOrSolvent(mol)) {
                        kind = "S";
                    }
                    else {
                        kind = "N";
                    }

                    // hk[0..3] hash keys
                    // hk[4] standardized smiles
                    for (int i = 0; i < hk.length; ++i)
                        re[i] = hk[i] + "-" +kind;

                    return re;
                }
            });
        return hk;
    }

    protected void lychify (Entity ent, Molecule mol) {
        try {
            Molecule clone = mol.cloneMolecule();
            Set<String> l3 = new TreeSet<>();
            Map<String, Molecule> l4 = new TreeMap<>();
            boolean lychify;
            
            Molecule[] frags = clone.convertToFrags();
            String[] moieties = new String[frags.length];
            for (int i = 0; i < frags.length; ++i) {
                Molecule f = frags[i];
                moieties[i] = f.toFormat("smiles:q");

                lychify = true;
                if (f.getAtomCount() == 1) {
                    // organic ion salt.. 
                    switch (f.getAtom(0).getAtno()) {
                    case 1: // H
                    case 6: // C
                    case 7: // N
                    case 8: // O
                    case 9: // F
                    case 11: // Na
                    case 15: // P
                    case 16: // S
                    case 17: // Cl
                    case 35: // Br
                    case 53: // I
                        lychify = false;
                        break;
                    }
                }
                
                if (lychify) {
                    String[] hk = lychify (f, false);
                    logger.info(hk[3]+": "+hk[4]);
                    f.setProperty(H_LyChI_L4.name(), hk[3]);
                    f.setProperty(LYCHI, hk[4]);
                    l3.add(hk[2]);
                    l4.put(hk[hk.length-1], f);
                }
            }
            ent._snapshot(MOIETIES, moieties);

            if (!l3.isEmpty()) {
                ent._set(H_LyChI_L3,
                         new StitchValue (l3.toArray(new String[0])));
            }

            if (!l4.isEmpty()) {
                String[] hk = new String[l4.size()];
                String[] ly = new String[l4.size()];
                int i = 0;
                for (Molecule f : l4.values()) {
                    hk[i] = f.getProperty(H_LyChI_L4.name());
                    ly[i] = f.getProperty(LYCHI);
                    ++i;
                }
                ent._set(H_LyChI_L4, new StitchValue (hk));
                ent._snapshot(LYCHI, ly); // store the lychi smiles
            }
            else {
                clone = mol.cloneMolecule();
                String[] hk = lychify (clone, false);
                ent._set(H_LyChI_L4, new StitchValue (hk[3]));
                ent._snapshot(LYCHI, hk[4]);
            }
            
            // with salt + solvent
            clone = mol.cloneMolecule();
            String[] hk = lychify (clone, false);
            if (hk != null)
                ent._set(H_LyChI_L5, new StitchValue (hk[3]));
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't generate LyChI hash for entity "
                       +ent.getId(), ex);
            firePropertyChange ("error", ent, ex);
        }
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
    
    protected Node _createNode () {
        if (source == null) {
            throw new IllegalStateException
                ("Can't create entity without a data source!");
        }
        
        Node node = gdb.createNode(AuxNodeType.ENTITY,
                                   Label.label(source.getName()));
        node.setProperty(SOURCE, source.getKey());
        return node;
    }
    
    protected Node createNode () {
        try (Transaction tx = gdb.beginTx()) {
            Node node = _createNode ();
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
        if (idField != null)
            ds.set("IdField", idField);
        if (nameField != null)
            ds.set("NameField", nameField);
        if (strucField != null)
            ds.set("StrucField", strucField);
    }
    
    public void updateDataSourceMetadata () {
        updateMeta (source);
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

    public DataSource register (String key, String name, File file) throws IOException {
        logger.warning("I'm EntityRegistry");
        return source = getDataSourceFactory().register(key, name, file);
    }

    public DataSource register (String name, File file) throws IOException {
        logger.warning("I'm EntityRegistry");
        return source = getDataSourceFactory().register(name, file);
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
