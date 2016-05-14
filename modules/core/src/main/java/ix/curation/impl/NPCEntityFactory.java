package ix.curation.impl;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.File;
import java.io.IOException;

import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.Util;

public class NPCEntityFactory extends MoleculeEntityFactory {
    static private final Logger logger =
        Logger.getLogger(NPCEntityFactory.class.getName());
    
    public NPCEntityFactory(GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public NPCEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public NPCEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setId("ID").
            add(StitchKey.I_CAS, "CAS").
            add(StitchKey.N_Name, "Synonyms").
            add(StitchKey.I_UNII, "CompoundUNII").
            add(StitchKey.T_Keyword, "DATASET");
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ix.curation.impl.NPCEntityFactory DB FILES...");
            System.exit(1);
        }
        
        NPCEntityFactory nef = new NPCEntityFactory (argv[0]);
        for (int i = 1; i < argv.length; ++i) {
            int pos = argv[i].indexOf('=');
            if (pos > 0) {
                String name = argv[i].substring(0, pos);
                if (name.equalsIgnoreCase("cache")) {
                    nef.setCache(argv[i].substring(pos+1));
                }
                else {
                    logger.warning("** Unknown parameter \""+name+"\"!");
                }
            }
            else {
                logger.info("***** registering "+argv[i]+" ******");
                nef.register(argv[i]);
            }
        }
        nef.shutdown();
    }
}
