package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class InxightEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(InxightEntityFactory.class.getName());
    
    public InxightEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public InxightEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public InxightEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setStrucField ("CompoundSmiles");
        add (N_Name, "CompoundName")
            .add(N_Name, "ConditionName")
            .add(N_Name, "ConditionDoValue")
            .add(N_Name, "ConditionMeshValue")
            .add(N_Name, "OfflabelUse")
            .add(I_UNII, "Unii")
            .add(I_CAS, "Cas")
            .add(I_NCT, "ClinicalTrial")
            .add(I_CODE, "ConditionDoId")
            .add(I_CODE, "ConditionMeshId")
            .add(T_Keyword, "HighestPhase")
            .add(T_Keyword, "TreamentModality")
            .add(T_Keyword, "type")
            ;
    }

    protected int register (InputStream is) throws IOException {
        LineTokenizer tokenizer = new LineTokenizer ('\t');
        tokenizer.setInputStream(is);
        String[] header = null;
        int count = 0, lines = 0;
        
        Map<String, Object> drug = new TreeMap<>();
        Map<String, Object> condition = new TreeMap<>();
        Map<String, Object> data = new TreeMap<>();
        
        for (; tokenizer.hasNext(); ++lines) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else if (toks.length != header.length) {
                logger.warning(lines+": expecting "+header.length
                               +" columns but instead found "+toks.length+"!");
            }
            else {
                drug.clear();
                condition.clear();
                data.clear();
                for (int i = 0; i < header.length; ++i) {
                    String value = toks[i];
                    if (value == null
                        || "Unknown".equalsIgnoreCase(value)
                        || "".equals(value)) {
                    }
                    else {
                        switch (header[i]) {
                        case "CompoundName":
                        case "CompoundSmiles":
                        case "Unii":
                        case "Cas":
                        case "OfflabelUse":
                        case "OfflabelUseUri":
                        case "OfflabelUseComment":
                            drug.put(header[i], value);
                            break;
                            
                        default:
                            if (header[i].indexOf("Condition") >= 0) {
                                if (header[i].equals("ConditionDoId"))
                                    value = "DOID:"+value;
                                else if (header[i].equals("ConditionMeshId"))
                                    value = "MESH:"+value;
                                condition.put(header[i], value);
                            }
                            else {
                                if (header[i].equals("ClinicalTrial")) {
                                    if (value.startsWith("NCT"))
                                        data.put(header[i], value);
                                    else
                                        data.put("ClinicalTrialRef", value);
                                }
                                else {
                                    data.put(header[i], value);
                                }
                            }
                        }
                    }
                }
                data.put("ConditionUri", condition.remove("ConditionUri"));

                Entity drugent = null;
                if (drug.containsKey("Unii")) {
                    drugent = getEntity ("Unii", drug.get("Unii"));
                }
                else {
                    drugent = getEntity ("CompoundName",
                                         drug.get("CompoundName"));
                }
                
                if (drugent == null) {
                    drug.put("type", "Drug");
                    drugent = register (drug);
                    logger.info("#### Drug \""+drug.get("CompoundName")
                                +"\" registered ("+drugent.getId()+") ####");
                }
                
                Entity disent = getEntity ("ConditionName",
                                           condition.get("ConditionName"));
                if (disent == null) {
                    condition.put("type", "Condition");
                    disent = register (condition);
                    logger.info("#### Condition \""
                                +condition.get("ConditionName")
                                +"\" registered ("+disent.getId()+") ####");
                }

                disent.stitch(drugent, R_rel, "indication_of", data);
            }
        }
        return count;
    }

    Entity getEntity (String prop, Object value) {
        Iterator<Entity> iter = find (prop, value);
        if (iter.hasNext())
            return iter.next();
        return null;
    }

    @Override
    public DataSource register (File file) throws IOException {
        DataSource ds = super.register(file);
        Integer count = (Integer)ds.get(INSTANCES);
        if (count == null) {
            count = register (ds.openStream());
            ds.set(INSTANCES, count);
            updateMeta (ds);
        }
        else {
            logger.info("### Data source "+ds.getName()+" ("+ds.getKey()+") "
                        +"is already registered with "+count+" entities!");
        }
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+InxightEntityFactory.class.getName()
                        +" DBDIR RANCHO-DISEASE-DRUG");
            System.exit(1);
        }
        
        try (InxightEntityFactory ief = new InxightEntityFactory (argv[0])) {
            ief.register(new File (argv[1]));
        }
    }
}
