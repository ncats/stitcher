package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;
import chemaxon.struc.Molecule;

public class SRSJsonEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(SRSJsonEntityFactory.class.getName());

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
        setId ("UNII");
        setUseName (false);
        add (N_Name, "Synonyms")
            .add (I_CAS, "CAS")
            .add (I_UNII, "UNII")
            //.add (T_ActiveMoiety, "ActiveMoieties")
            ;
    }

    @Override
    public int register (InputStream is) throws IOException {
        BufferedReader br = new BufferedReader (new InputStreamReader (is));
        int count = 0, ln = 0;
        Map<String, Entity> activeMoieties = new HashMap<>();
        Map<String, Set<Entity>> unresolved = new HashMap<>();
        for (String line; (line = br.readLine()) != null; ++ln) {
            System.out.println("+++++ "+(count+1)+"/"+(ln+1)+" +++++");
            String[] toks = line.split("\t");
            if (toks.length < 2) {
                logger.warning(ln+": Expecting 3 fields, but instead got "
                               +toks.length+";\n"+line);
                continue;
            }
            
            //logger.info("JSON: "+toks[2]);
            Molecule mol = Util.fromJson(toks[2]);
            if (mol != null) {
                Entity ent = register (mol);
                
                String moieties = mol.getProperty("ActiveMoieties");
                if (moieties != null) {
                    String[] actives = moieties.split("\n");
                    for (String a : actives) {
                        if (a.equals(mol.getProperty("UNII"))) {
                            activeMoieties.put(a, ent);
                        }
                        else if (activeMoieties.containsKey(a)) {
                            Entity e = activeMoieties.get(a);
                            // create manual stitch from e -> ent
                            if (!ent.equals(e))
                                ent.stitch(e, T_ActiveMoiety, a);
                        }
                        else {
                            Set<Entity> ents = unresolved.get(a);
                            if (ents == null)
                                unresolved.put(a, ents = new HashSet<>());
                            ents.add(ent);
                        }
                    }
                }
                ++count;
            }
        }
        br.close();

        logger.info("## "+unresolved.size()+" unresolved active moieties!");
        for (Map.Entry<String, Set<Entity>> me : unresolved.entrySet()) {
            Entity active = activeMoieties.get(me.getKey());
            if (active != null) {
                for (Entity e : me.getValue())
                    if (!active.equals(e))
                        e.stitch(active, T_ActiveMoiety, me.getKey());
            }
            else {
                int cnt = 0;
                for (Iterator<Entity> it = find (I_UNII, me.getKey());
                     it.hasNext(); ++cnt) {
                    active = it.next();
                    for (Entity e : me.getValue())
                        if (!active.equals(e))
                            e.stitch(active, T_ActiveMoiety, me.getKey());
                }
                
                if (cnt == 0)
                    logger.warning("** Unknown reference to active moiety: "
                                   +me.getKey());
            }
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
        try {
            for (int i = 1; i < argv.length; ++i) {
                int pos = argv[i].indexOf('=');
                if (pos > 0) {
                    String name = argv[i].substring(0, pos);
                    if (name.equalsIgnoreCase("cache")) {
                        mef.setCache(argv[i].substring(pos+1));
                    }
                    else {
                        logger.warning("** Unknown parameter \""+name+"\"!");
                    }
                }
                else {
                    mef.register(argv[i]);
                }
            }
        }
        finally {
            mef.shutdown();
        }
    }
}
