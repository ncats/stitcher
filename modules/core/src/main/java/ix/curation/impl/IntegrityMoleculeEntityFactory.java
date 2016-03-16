package ix.curation.impl;

import java.util.*;
import java.io.File;
import java.io.IOException;

import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.Util;
import ix.curation.StitchKeyMapper;

public class IntegrityMoleculeEntityFactory extends MoleculeEntityFactory {

    class KeyMapper implements StitchKeyMapper {
        final StitchKey key;
        KeyMapper (StitchKey key) {
            this.key = key;
        }
        
        /**
         * StitchKeyMapper interface
         */
        public Map<StitchKey, Object> map (Object value) {
            Set<String> values = new TreeSet<String>();
            for (String s : value.toString().split("[\r\n]+")) {
                int pos = s.indexOf('(');
                if (pos > 0 && s.charAt(pos-1) == ' ') {
                    s = s.substring(0, pos);
                }
                s = s.trim();
                if (s.length() > 0)
                    values.add(normalize (s, key));
            }
            
            Map<StitchKey, Object> map = new HashMap<StitchKey, Object>();
            map.put(key, values.isEmpty()
                    ? null : values.toArray(new String[0]));
            return map;
        }
    }
    
    public IntegrityMoleculeEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public IntegrityMoleculeEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public IntegrityMoleculeEntityFactory (File dir) throws IOException {
        super (dir);
    }


    @Override
    protected void init () {
        super.init();
        setId ("Prous_Science_Entry_Number");
        setUseName (false);
        add (StitchKey.T_Keyword, "Highest_Phase");
        add ("CAS", new KeyMapper (StitchKey.I_CAS));
        add ("Drug_Name", new KeyMapper (StitchKey.N_Name));
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: "+IntegrityMoleculeEntityFactory.class.getName()
                 +" DB [cache=DIR] FILES...");
            System.exit(1);
        }
        
        IntegrityMoleculeEntityFactory nef =
            new IntegrityMoleculeEntityFactory (argv[0]);
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
