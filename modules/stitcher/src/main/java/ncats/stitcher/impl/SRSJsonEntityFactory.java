package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;
import chemaxon.struc.Molecule;
import ncats.stitcher.calculators.events.GSRSEventParser;

public class SRSJsonEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(SRSJsonEntityFactory.class.getName());

    static String RELATIONSHIPS = "relationships"; // GSRS keyword denoting an active moiety relationship
    static Map<String, StitchKey> mappedRels = new HashMap<>();
    static {
        mappedRels.put("ACTIVE MOIETY", R_activeMoiety);
        // TODO These other relationships cause OutOfMemory ... and excessive traverses
        //mappedRels.put("SALT/SOLVATE->PARENT", R_rel);
        //mappedRels.put("PARENT->SALT/SOLVATE", R_rel);
        //mappedRels.put("METABOLITE ACTIVE->PARENT", R_rel);
        //mappedRels.put("PARENT->METABOLITE ACTIVE", R_rel);
        //mappedRels.put("METABOLITE ACTIVE->PRODRUG", R_rel);
        //mappedRels.put("PRODRUG->METABOLITE ACTIVE", R_rel);
    }
    Map<String, Entity> activeMoieties = new HashMap<>();
    Map<String, Set<Entity>> unresolved = new HashMap<>();
    int count = 0;
    
    public SRSJsonEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public SRSJsonEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public SRSJsonEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("UNII");
        setNameField ("PreferredName");
        setEventParser(GSRSEventParser.class.getCanonicalName());
        setUseName (false);
        add (N_Name, "Synonyms");
        add (I_CAS, "CAS");
        add (I_UNII, "UNII");
        add (I_CID, "PUBCHEM");
        add (T_Keyword, "Class");
    }

    void register (String line, int total) {
        System.out.println("+++++ "+(count+1)+"/"+total+" +++++");
        String[] toks = line.split("\t");
        if (toks.length < 2) {
            logger.warning(total+": Expecting 3 fields, but instead got "
                           +toks.length+";\n"+line);
            return;
        }
            
        //logger.info("JSON: "+toks[2]);
        Object vobj = Util.fromJson(toks[2]);
        if (vobj == null) {
            logger.warning("Can't parse json: "+toks[2]);
        }
        else if (vobj instanceof Molecule) {
            Molecule mol = (Molecule)vobj;
            for (int i = 0; i < mol.getPropertyCount(); ++i) {
                String prop = mol.getPropertyKey(i);
                properties.add(prop);
            }
            
            Entity ent = register (mol);
            String relationships = mol.getProperty(RELATIONSHIPS);
            if (relationships != null && relationships.length() > 0) {
                String[] rels = relationships.split("\n");
                for (String rel : rels) {
                    processGSRSRel(ent, rel, false);
                }
            }

            ++count;
        }
        else { // not chemical
            Map<String, Object> map = (Map)vobj;
            properties.addAll(map.keySet());
            
            Entity ent = register (map);
            vobj = map.get(RELATIONSHIPS);
            if (vobj != null) {
                String unii = (String)map.get("UNII");
                if (vobj.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(vobj); ++i) {
                        processGSRSRel(ent, (String)Array.get(vobj, i), false);
                    }
                }
                else if (unii != null && ((String)vobj).contains(unii)) {
                    activeMoieties.put(unii, ent);
                }
                else {
                    processGSRSRel(ent, (String)vobj, false);
                }
            }
            ++count;
        }
    }

    private void processGSRSRel(Entity ent, String rel, boolean force) {
        String entry[] = rel.split("\\|");
        String type = entry[0];
        if (entry.length == 1) {
            System.err.println("oops");
            return;
        }
        String id = entry[1];
        StitchKey link;
        if (mappedRels.containsKey(type))
            link = mappedRels.get(type);
        else return;
        Map<String, Object> attrs = new HashMap<>();
        attrs.put(type, id);
        if (activeMoieties.containsKey(id)) {
            Entity e = activeMoieties.get(id);
            // create manual stitch from ent -> e
            if (!ent.equals(e))
                ent.stitch(e, link, id, attrs);
        } if (force) {
            int cnt = 0;
            for (Iterator<Entity> it = find (I_UNII, id);
                 it.hasNext(); ++cnt) {
                Entity active = it.next();
                if (!active.equals(ent))
                    ent.stitch(active, link, id, attrs);
            }

            if (cnt == 0)
                logger.warning("** Unknown reference to "+type+": "
                        +id+":"+rel);
        } else {
            Set<Entity> ents = unresolved.get(rel);
            if (ents == null)
                unresolved.put(rel, ents = new HashSet<>());
            ents.add(ent);
        }
    }

    @Override
    public int register (InputStream is) throws IOException {
        count = 0;
        unresolved.clear();
        activeMoieties.clear();
        
        BufferedReader br = new BufferedReader (new InputStreamReader (is));
        int ln = 0;
        for (String line; (line = br.readLine()) != null; ++ln) {
            try {
                register (line, ln+1);
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, "can't register entry: "+line, ex);
            }
        }
        br.close();

        logger.info("## "+unresolved.size()+" unresolved active moieties!");
        for (Map.Entry<String, Set<Entity>> me : unresolved.entrySet()) {
            for (Entity ent : me.getValue())
                processGSRSRel(ent, me.getKey(), true);
//            String rel = me.getKey();
//            String entry[] = rel.split("\\|");
//            String type = entry[0];
//            String id = entry[1];
//            StitchKey link = R_rel;
//            if (type.equals("ACTIVE MOIETY"))
//                link = R_activeMoiety;
//            Entity active = activeMoieties.get(id);
//            if (active != null) {
//                for (Entity e : me.getValue())
//                    if (!active.equals(e))
//                        e.stitch(active, link, id);
//            }
//            else {
//                int cnt = 0;
//                for (Iterator<Entity> it = find (I_UNII, id);
//                     it.hasNext(); ++cnt) {
//                    active = it.next();
//                    for (Entity e : me.getValue())
//                        if (!active.equals(e))
//                            e.stitch(active, link, id);
//                }
//
//                if (cnt == 0)
//                    logger.warning("** Unknown reference to active moiety: "
//                                   +id+":"+me.getKey());
//            }
        }

        return count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+SRSJsonEntityFactory.class.getName()
                               +" DBDIR [cache=DIR] FILE...");
            System.exit(1);
        }

        SRSJsonEntityFactory mef = new SRSJsonEntityFactory (argv[0]);
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
                        mef.register(file.getName(), file);
                    }
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }
}
