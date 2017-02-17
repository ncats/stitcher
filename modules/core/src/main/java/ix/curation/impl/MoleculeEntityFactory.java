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

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Transaction;

import ix.curation.*;

public class MoleculeEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(MoleculeEntityFactory.class.getName());

    public static final String SMILES = "SMILES";
    public static final String MOLFILE = "MOLFILE";
    public static final String ACTION_TYPE = "ActionType";

    protected boolean useName; // use mol's name
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
        super.init();
        properties = new TreeSet<String>();
        useName = true;
    }

    public MoleculeEntityFactory setUseName (boolean useName) {
        this.useName = useName;
        return this;
    }

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
        if (idField != null) {
            idval = mol.getProperty(idField);
        }

        Entity ent = Entity._getEntity(_createNode ());
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
        if (!mappers.isEmpty()) {
            Map<String, Object> values = new HashMap<String, Object>();
            for (String key : mappers.keySet()) {
                String v = mol.getProperty(key);
                if (v != null)
                    values.put(key, v);
            }
            mapValues (ent, values);
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

    @Override
    protected void updateMeta (DataSource ds) {
        super.updateMeta(ds);
        ds.set(PROPERTIES, properties.toArray(new String[0]));
    }
    
    public static void main(String[] argv) throws Exception {
        register (argv, MoleculeEntityFactory.class);
    }
}
