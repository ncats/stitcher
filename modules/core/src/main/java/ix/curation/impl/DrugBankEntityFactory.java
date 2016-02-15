package ix.curation.impl;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.io.File;
import java.io.IOException;

import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.Util;
import ix.curation.StitchKeyMapper;

public class DrugBankEntityFactory extends MoleculeEntityFactory {
    static private final Logger logger =
        Logger.getLogger(DrugBankEntityFactory.class.getName());

    class KeyMapper implements StitchKeyMapper {
        StitchKey key;
        KeyMapper (StitchKey key) {
            this.key = key;
        }
        public Map<StitchKey, Object> map (String value) {
            Set<String> values = new TreeSet<String>();
            for (String line : value.split("[\r\n]+")) {
                for (String tok : line.split(";")) {
                    values.add(normalize (tok.trim(), key));
                }
            }
            
            Map<StitchKey, Object> map = new TreeMap<StitchKey, Object>();
            if (!values.isEmpty()) {
                //logger.info("Mapping: "+key+" => "+value+" => "+values);
                map.put(key, values.toArray(new String[0]));
            }
            
            return map;
        }
    }
    
    public DrugBankEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public DrugBankEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public DrugBankEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setId ("DRUGBANK_ID");
        setUseName (false);
        add (StitchKey.H_InChIKey, "INCHI_KEY");
        add (StitchKey.N_Synonym, "GENERIC_NAME");
        add (StitchKey.N_Synonym, "JCHEM_TRADITIONAL_IUPAC");
        add ("DRUG_GROUPS", new KeyMapper (StitchKey.T_Keyword));
        add ("SYNONYMS", new KeyMapper (StitchKey.N_Synonym));
        add ("INTERNATIONAL_BRANDS", new KeyMapper (StitchKey.N_Synonym));
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ix.curation.impl.DrugBankEntityFactory DB FILES...");
            System.exit(1);
        }
        
        DrugBankEntityFactory nef = new DrugBankEntityFactory (argv[0]);
        for (int i = 1; i < argv.length; ++i) {
            logger.info("***** registering "+argv[i]+" ******");
            nef.register(argv[i]);
        }
        nef.shutdown();
    }
}
