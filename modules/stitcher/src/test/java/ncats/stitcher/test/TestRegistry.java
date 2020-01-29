package ncats.stitcher.test;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;


public class TestRegistry extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(TestRegistry.class.getName());
    
    public TestRegistry (String name) throws Exception {
        super (GraphDb.createTempDb(name));
        // set data source for this registry
        setDataSource(getDataSourceFactory().register(name));
        updateDataSourceMetadata ();            
    }

    @Override
    protected void init () {
        super.init();
        //setStrucField("MOLFILE");
        setStrucField ("SMILES");
        setIdField ("id");
        // setup mapping
        add (I_UNII, "UNII");
        add (I_UNII, "Unii");
        add (I_UNII, "unii");
        add (I_UNII, "CompoundUNII");           
        add (I_CAS, "CAS");
        add (I_CAS, "Cas");
        add (I_CID, "PUBCHEM");
        add (I_CID, "Cid");
        add (I_ChEMBL, "ChemblId");
        add (I_ChEMBL, "ChEMBL");
        add (I_DB, "DrugbankId");
        add (I_DB, "drugbank-id");
        add (I_DB, "DRUG BANK");        
        add (N_Name, "Synonyms");       
        add (N_Name, "CompoundName");
        add (N_Name, "CompoundSynonym");
        add (N_Name, "Drug Substance");
        add (N_Name, "Chemical Name");
        add (T_Keyword, "Class");
        add (T_Keyword, "DATASET");
        add (T_Keyword, "Therapeutic Function");
    }

    public void register (ArrayNode data) {
        Map<Object, Entity> entities = new HashMap<>();
        Map<Object, Object> activeMoieties = new HashMap<>();
        for (int i = 0; i < data.size(); ++i) {
            Map<String, Object> row = new HashMap<>();
            for (Iterator<Map.Entry<String, JsonNode>> it =
                     data.get(i).fields(); it.hasNext(); ) {
                Map.Entry<String, JsonNode> me = it.next();
                JsonNode n = me.getValue();
                    
                if (n.isArray()) {
                    ArrayNode an = (ArrayNode)n;
                    String[] vals = new String[an.size()];
                    for (int j = 0; j < an.size(); ++j) {
                        StitchKey key = getStitchKey (me.getKey());
                        if (key != null) {
                            switch (key) {
                            case N_Name:
                            case I_DB:
                            case I_ChEMBL:
                            case I_UNII:
                                vals[j] = an.get(j).asText().toUpperCase();
                                break;
                            default:
                                vals[j] = an.get(j).asText();
                            }
                        }
                        else
                            vals[j] = an.get(j).asText();
                    }
                    row.put(me.getKey(), vals);
                }
                else {
                    String v = n.asText();
                    if (v.length() > 0)
                        row.put(me.getKey(), v);
                }
            }
                
            Entity e = register (row);
            if (row.containsKey("PreferredName")) {
                // only use id from gsrs entries (which has PreferredName)
                Object id = row.get("id");
                entities.put(id, e);
                Object am = row.get("ActiveMoieties");
                if (am != null)
                    activeMoieties.put(id, am);
            }
        }
            
        for (Map.Entry<Object, Object> me : activeMoieties.entrySet()) {
            Entity child = entities.get(me.getKey());
            Object am = me.getValue();
            if (am.getClass().isArray()) {
                int len = Array.getLength(am);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(am, i);
                    Entity parent = entities.get(v);
                    if (parent == null) {
                        logger.warning("Can't lookup parent entity: "+v);
                    }
                    else if (!child.equals(parent))
                        child.stitch(parent, R_activeMoiety, v);
                }
            }
            else {
                Entity parent = entities.get(am);
                if (!child.equals(parent))
                    child.stitch(parent, R_activeMoiety, am);
            }
        }
    }

    public Map<Integer, Integer> calcScoreHistogram () {
        Map<Integer, Integer> scores = new TreeMap<>();
        traverse ((traversal, triple) -> {
                Map<StitchKey, Object> values = triple.values();
                int score = 0;
                for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
                    Object v = me.getValue();
                    if (v != null) {
                        int s = v.getClass().isArray() ? Array.getLength(v) : 1;
                        score += me.getKey().priority*s;
                    }
                }
                Integer c = scores.get(score);
                scores.put(score, c == null ? 1 : (c+1));
                
                return true;
            });
        return scores;
    }


    public DataSource getDataSourceByVersion(int version){
        return getDataSourceFactory().register("stitch_v" + version);
    }
}

