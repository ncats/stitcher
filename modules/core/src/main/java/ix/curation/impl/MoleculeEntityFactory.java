package ix.curation.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import chemaxon.struc.Molecule;
import chemaxon.formats.MolImporter;

import ix.curation.*;
import lychi.LyChIStandardizer;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.GraphDatabaseService;


public class MoleculeEntityFactory extends EntityRegistry<Molecule> {
    static final Logger logger =
            Logger.getLogger(MoleculeEntityFactory.class.getName());

    public static final String SMILES = "SMILES";
    public static final String MOLFILE = "MOLFILE";
    public static final String ACTION_TYPE = "ActionType";

    protected EnumMap<StitchKey, Set<String>> stitches;
    protected String id; // property id
    protected boolean useName; // use mol's name
    protected Map<String, StitchKeyMapper> mappers;
    // stitch key due to mappers
    protected EnumMap<StitchKey, Set<String>> stitchMappers;
    protected Set<String> properties;

    public MoleculeEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    public MoleculeEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public MoleculeEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        stitches = new EnumMap<>(StitchKey.class);
        mappers = new HashMap<String, StitchKeyMapper>();
        stitchMappers = new EnumMap<>(StitchKey.class);
        properties = new TreeSet<String>();
        useName = true;
    }

    public void clear () { stitches.clear(); }
    public MoleculeEntityFactory add (StitchKey key, String property) {
        Set<String> props = stitches.get(key);
        if (props == null) {
            stitches.put(key, props = new TreeSet<String>());
        }
        props.add(property);
        return this;
    }
    
    public MoleculeEntityFactory add (String property, StitchKeyMapper mapper) {
        mappers.put(property, mapper);
        return this;
    }

    public Set<String> get (StitchKey key) {
        return stitches.get(key);
    }

    public MoleculeEntityFactory setId (String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return id;
    }
    
    public MoleculeEntityFactory setUseName (boolean useName) {
        this.useName = useName;
        return this;
    }

    @Override
    public Entity register (final Molecule mol) {
        // execute in transaction context
        try (Transaction tx = gdb.beginTx()) {
            Entity e = _register (mol);
            tx.success();
            firePropertyChange ("entity", mol, e);
            
            return e;
        }
    }

    public Entity _register (final Molecule mol) {
        String idval =  null;
        // add unique identifier to node (source is already present as label?)
        if (id != null) {
            idval = mol.getProperty(id);
        }

        Entity ent = Entity._getEntity(_createNode (EntityType.Agent));
        DefaultPayload payload = new DefaultPayload (getDataSource ());

        if (idval != null) {
            //ent._addLabel(DynamicLabel.label(idLabel));
            ent._snapshot(ID, idval);
            payload.setId(idval);
        }

        String name = mol.getName();
        if (name != null) {
            name = name.trim();
            if (!name.equals("")) {
                if (useName) {
                    ent._add(StitchKey.N_Name, new StitchValue
                             (NAME, normalize (name, StitchKey.N_Name)) {
                            @Override
                            public Object getDisplay() {
                                return getValue();
                            }
                        });
                }
                
                if (idval == null) {
                    payload.setId(name);
                    ent._snapshot(ID, name);
                }
                payload.put(NAME, name, true);          
            }
        }

        for (Map.Entry<StitchKey, Set<String>> me : stitches.entrySet()) {
            switch (me.getKey()) {
            case I_SID:
            case I_CID:
            case I_PMID:
                for (String prop : me.getValue()) {
                    String value = mol.getProperty(prop);
                    if (value != null) {
                        Set<Long> ids = new TreeSet<Long>();
                        for (String t : value.split("[\n\\s,]+")) {
                            try {
                                long lv = Long.parseLong(t);
                                ids.add(lv);
                            } catch (NumberFormatException ex) {
                                ex.printStackTrace();
                            }
                        }
                        Object ary = ids.toArray(new Long[0]);
                        ent._add(me.getKey(), new StitchValue (prop, ary));
                        payload.put(prop, ary);
                    }
                }
                break;

            case T_Keyword:
                for (String prop : me.getValue()) {
                    String value = mol.getProperty(prop);
                    if (value != null) {
                        for (String t : value.split("[\n]+")) {
                            ent._addLabel(DynamicLabel.label(t));
                        }
                        payload.put(prop, value.split("[\n]+"));
                    }
                }
                break;

            default:
                for (String prop : me.getValue()) {
                    String value = mol.getProperty(prop);
                    if (value != null) {
                        List<String> values = new ArrayList<String>();
                        for (String t : value.split("[\r\n]+")) {
                            String tidy = t.trim();
                            if (tidy.length() > 0)
                                values.add(tidy);
                        }

                        if (!values.isEmpty()) {
                            Set<String> normalized = new TreeSet<String>();
                            for (String t : values)
                                normalized.add(normalize(t, me.getKey()));
                            try {
                                ent._add(me.getKey(), new StitchValue
                                         (prop,
                                          normalized.toArray(new String[0])));
                            }
                            catch (Exception ex) {
                                logger.log(Level.SEVERE, prop+" => "
                                           +me.getKey()+" \""+value+"\"", ex);
                            }
                            payload.put
                                (prop, values.toArray(new String[0]));
                        }
                    }
                }
            } // switch
        }

        // now add any mapper
        stitchMappers.putAll(stitches);
        for (Map.Entry<String, StitchKeyMapper> me : mappers.entrySet()) {
            String value = mol.getProperty(me.getKey());
            if (value != null) {
                Map<StitchKey, Object> mapping = me.getValue().map(value);
                for (Map.Entry<StitchKey, Object> m : mapping.entrySet()) {
                    //logger.info("mapper "+m.getKey()+" => "+m.getValue());
                    Object val = m.getValue();
                    if (val == null) {
                    }
                    else if (m.getKey() == StitchKey.T_Keyword) {
                        if (val.getClass().isArray()) {
                            int len = Array.getLength(val);
                            for (int i = 0; i < len; ++i) {
                                Object t = Array.get(val, i);
                                ent._addLabel(DynamicLabel.label(t.toString()));
                            }
                        }
                        else
                            ent._addLabel(DynamicLabel.label(val.toString()));
                    }
                    else {
                        ent._add(m.getKey(),
                                 new StitchValue (me.getKey(), val));
                    }
                    
                    Set<String> props = stitchMappers.get(m.getKey());
                    if (props == null) {
                        stitchMappers.put(m.getKey(),
                                          props = new TreeSet<String>());
                    }
                    props.add(me.getKey());
                }
                payload.put(me.getKey(), value);
            }
        }

        // now store all original properties..
        for (int i = 0; i < mol.getPropertyCount(); ++i) {
            String prop = mol.getPropertyKey(i);
            String value = mol.getProperty(prop);
                            
            if (value != null && !payload.has(prop)) {
                List<String> values = new ArrayList<String>();
                int max = 0;
                for (String s : value.split("\n")) {
                    String v = s.trim();
                    int len = v.length();
                    if (len > 0) {
                        if (len > max) {
                            max = len;
                        }
                        values.add(v);
                    }
                }
                // don't index if the string content is too long
                if (!values.isEmpty())
                    payload.put(prop, values.toArray(new String[0]), max < 100);
            }
            properties.add(prop);
        }
        
        payload.put(MOLFILE, mol.toFormat("mol"), false);
        try {
            payload.put(SMILES, mol.toFormat("smiles"), false);
        }
        catch (Exception ex) {
            logger.warning("Can't export structure as smiles!");
        }

        if (mol.getAtomCount() > 0)
            lychify (ent, mol);

        ent._add(payload);

        return ent;
    }

    /*
     * TODO: do something more intelligent here
     */
    protected String normalize (String value, StitchKey key) {
        return value.toUpperCase();
    }

    private String[] lychify (final Molecule mol, final boolean stripSalt)
        throws Exception {
        String hash = Util.sha1hex("LyChI:" + mol.exportToFormat("smiles"));
        if (stripSalt == false)
            hash = Util.sha1hex("LyChISalt:" + mol.exportToFormat("smiles"));

        String[] hk = getCache().getOrElse(hash, new Callable<String[]> () {
                public String[] call () throws Exception {
                    LyChIStandardizer lychi = new LyChIStandardizer ();
                    /*
                     * don't strip salt/solvent if the structure has metals
                     */
                    lychi.removeSaltOrSolvent
                    (stripSalt && !LyChIStandardizer.containMetals(mol));
                    lychi.standardize(mol);
                    return LyChIStandardizer.hashKeyArray(mol);
                }
            });
        return hk;
    }

    protected void lychify (Entity ent, Molecule mol) {
        try {
            Molecule stdmol = mol.cloneMolecule();
            String[] hk = lychify (stdmol, true);
            if (hk != null) {
                ent._set(StitchKey.H_LyChI_L3, new StitchValue (hk[2]));
                ent._set(StitchKey.H_LyChI_L4, new StitchValue (hk[3]));
            }
            
            // with salt + solvent
            stdmol = mol.cloneMolecule();
            hk = lychify (stdmol, false);
            if (hk != null)
                ent._set(StitchKey.H_LyChI_L5, new StitchValue (hk[3]));
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't generate LyChI hash for entity "
                       +ent.getId(), ex);
            firePropertyChange ("error", ent, ex);
        }
    }

    public int register (InputStream is) throws IOException {
        int count = 0;
        MolImporter mi = new MolImporter (is);  
        for (Molecule mol; (mol = mi.read()) != null; ++count) {
            System.out.println("+++++ "+(count+1)+" +++++");
            register (mol);
        }
        mi.close();
        logger.info("$$$ "+count+" entities registered!");
        return count;
    }

    public DataSource register (String file) throws IOException {
        return register (new File (file));
    }

    public int register (DataSource ds) throws IOException {
        Integer instances = (Integer)ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            instances = register (ds.openStream());
            ds.set(INSTANCES, instances);
            updateMeta (ds);
            logger.info("$$$ "+instances+" entities registered for "+ds);
        }
        return instances;
    }
    
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        register (ds);
        return ds;
    }

    public DataSource register (URL url) throws IOException {
        DataSource ds = super.register(url);
        register (ds);
        return ds;
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
        ds.set(PROPERTIES, properties.toArray(new String[0]));
    }
    
    public static void main(String[] argv) throws Exception {
        if (argv.length < 3) {
            System.err.println("Usage: " + MoleculeEntityFactory.class.getName()
                    + " [DBDIR] [SourceName] [FILE|URL] PROPS...");
            System.exit(1);
        }

        MoleculeEntityFactory mef = new MoleculeEntityFactory (argv[0]);
        for (int i = 3; i < argv.length; ++i) {
            String[] toks = argv[i].split(":");
            if (toks.length == 2) {
                try {
                    if ("id".equalsIgnoreCase(toks[0])) {
                        mef.setId(toks[1]);
                        logger.info("id property: " + toks[1]);
                    } else {
                        StitchKey key = StitchKey.valueOf(toks[0]);
                        mef.add(key, toks[1]);
                        logger.info(key + " => \"" + toks[1] + "\"");
                    }
                } catch (Exception ex) {
                    logger.warning(ex.getMessage());
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
