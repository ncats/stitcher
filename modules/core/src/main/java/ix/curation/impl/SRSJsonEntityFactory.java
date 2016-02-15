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
        add (StitchKey.N_Synonym, "Synonyms");
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
    
    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        Integer instances = (Integer)ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            int count = register (ds.openStream());
            ds.set(INSTANCES, count);
            logger.info("$$$ end processing "+file.getName()
                        +"; "+count+" entities registered!");
        }
        return ds;
    }

    @Override
    public DataSource register (URL url) throws IOException {
        DataSource ds = super.register(url);
        Integer instances = (Integer)ds.get(INSTANCES);
        if (instances != null) {
            logger.warning("### Data source "+ds.getName()
                           +" has already been registered with "+instances
                           +" entities!");
        }
        else {
            int count = register (ds.openStream());
            ds.set(INSTANCES, count);
            logger.info("$$$ end processing "+url
                        +"; "+count+" entities registered!");
        }
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+SRSJsonEntityFactory.class.getName()
                               +" DBDIR FILE...");
            System.exit(1);
        }

        SRSJsonEntityFactory mef = new SRSJsonEntityFactory (argv[0]);
        try {
            for (int i = 1; i < argv.length; ++i) {
                mef.register(argv[i]);
            }
        }
        finally {
            mef.shutdown();
        }
    }
}
