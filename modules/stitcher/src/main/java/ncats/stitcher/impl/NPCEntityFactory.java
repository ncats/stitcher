package ncats.stitcher.impl;

import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.File;
import java.io.IOException;

import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;

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
        setIdField ("ID");
        add (StitchKey.I_CAS, "CAS");
        //add (StitchKey.N_Name, "Synonyms");
        add (StitchKey.I_UNII, "CompoundUNII");
        add (StitchKey.T_Keyword, "DATASET");
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ncats.stitcher.impl.NPCEntityFactory DB FILES...");
            System.exit(1);
        }
        
        NPCEntityFactory mef = new NPCEntityFactory (argv[0]);
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
                        mef.register(file);
                    }
                }
            }
        }
        finally{
            mef.shutdown();
        }
    }
}
