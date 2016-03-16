package ix.curation.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import ix.curation.*;
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
        add (StitchKey.N_Name, "Synonyms");
        add (StitchKey.I_CAS, "CAS");
        add (StitchKey.I_UNII, "UNII");
    }

    @Override
    public int register (InputStream is) throws IOException {
        BufferedReader br = new BufferedReader (new InputStreamReader (is));
        int count = 0, ln = 0;
        for (String line; (line = br.readLine()) != null; ++ln) {
            System.out.println("+++++ "+(count+1)+"/"+(ln+1)+" +++++");
            String[] toks = line.split("\t");
            
            //logger.info("JSON: "+toks[2]);
            Molecule mol = Util.fromJson(toks[2]);
            if (mol != null) {
                register (mol);
                ++count;
            }
        }
        br.close();        
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
