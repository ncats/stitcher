package ncats.stitcher.test;

import java.util.*;
import java.io.*;
import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class TestStitcher {
    static final Logger logger =
        Logger.getLogger(TestStitcher.class.getName());

    static class TestRegistry extends EntityRegistry {
        TestRegistry (String name) throws Exception {
            super (GraphDb.createTempDb(name));
            // set data source for this registry
            setDataSource(getDataSourceFactory().register(name));
        }

        @Override
        protected void init () {
            super.init();
            //setStrucField("MOLFILE");
            setStrucField("SMILES");
            // setup mapping
            add(I_UNII, "UNII");
            add(N_Name, "Synonyms");
            add(I_CAS, "CAS");
            add(I_CID, "PUBCHEM");
            add(N_Name, "CompoundName");
            add(N_Name, "CompoundSynonym");
            add(I_CAS, "Cas");
            add(I_UNII, "CompoundUNII");
            add(I_CID, "Cid");
            add(T_Keyword, "Class");
            add(T_Keyword, "DATASET");
        }

        void register (ArrayNode data) {
            Map<String, Entity> entities = new HashMap<>();
            Map<String, String> activeMoieties = new HashMap<>();
            for (int i = 0; i < data.size(); ++i) {
                Map<String, Object> row = new HashMap<>();
                for (Iterator<Map.Entry<String, JsonNode>> it =
                         data.get(i).fields(); it.hasNext(); ) {
                    Map.Entry<String, JsonNode> me = it.next();
                    JsonNode n = me.getValue();
                    
                    if (n.isArray()) {
                        ArrayNode an = (ArrayNode)n;
                        String[] vals = new String[an.size()];
                        for (int j = 0; j < an.size(); ++j)
                            vals[j] = an.get(j).asText();
                        row.put(me.getKey(), vals);
                    }
                    else {
                        row.put(me.getKey(), n.asText());
                    }
                }
                
                Entity e = register (row);
                String id = (String) row.get("id");
                if (id != null) {
                    entities.put(id, e);
                    Object am = row.get("ActiveMoieties");
                    if (am != null) {
                        if (am.getClass().isArray())
                            am = Array.get(am, 0);
                        
                        if (!id.equals(am))
                            activeMoieties.put(id, (String)am);
                    }
                }
            }
            
            for (Map.Entry<String, String> me : activeMoieties.entrySet()) {
                Entity child = entities.get(me.getKey());
                Entity parent = entities.get(me.getValue());
                child.stitch(parent, T_ActiveMoiety, me.getValue());
            }
        }
    }

    void testMergedStitches (String name, InputStream... streams)
        throws Exception {
        ObjectMapper mapper = new ObjectMapper ();
        ArrayNode data = mapper.createArrayNode();

        int total = 0;
        for (InputStream is : streams) {
            JsonNode json = mapper.readTree(is);
            total += json.get("count").asInt();
            ArrayNode an = (ArrayNode)json.get("data");
            for (int i = 0; i < an.size(); ++i)
                data.add(an.get(i));
        }
        assertTrue ("Expecting json to contain 6 elements in data but "
                    +"instead got "+data.size(), data.size() == total);

        TestRegistry reg = new TestRegistry (name);
        reg.register(data);
        
        long count = reg.count(AuxNodeType.ENTITY);
        assertTrue ("Expecting "+data.size()+" entities but instead got "
                    +count, data.size() == count);

        DataSource ds = reg.getDataSourceFactory().register(name+"-stitch");
        List<Long> comps = new ArrayList<>();
        int nc = reg.components(comps);
        assertTrue ("Expect 1 component but instead got "+nc, nc == 1);

        for (Long id : comps) {
            Component comp = reg.component(id);
            reg.untangle(new UntangleCompoundComponent (ds, comp));
        }
        
        count = reg.count(ds.getName());
        assertTrue ("Expect 1 stitch node but instead got "+count, count == 1l);

        reg.shutdown();
    }
    

    @Rule public TestName name = new TestName();
    public TestStitcher () {
    }

    @Test
    public void testStitch1 () throws Exception {
        logger.info("##### "+name.getMethodName()+" #####");
        testMergedStitches
            (name.getMethodName(),
             EntityRegistry.class.getResourceAsStream("/1JQS135EYN.json"));
    }

    @Test
    public void testStitch2 () throws Exception {
        logger.info("##### "+name.getMethodName()+" #####");
        testMergedStitches
            (name.getMethodName(),
             EntityRegistry.class.getResourceAsStream("/cefotetan1.json"),
             EntityRegistry.class.getResourceAsStream("/cefotetan2.json"));
    }

    @Test
    public void testStitch3 () throws Exception {
        logger.info("##### "+name.getMethodName()+" #####");
        testMergedStitches
            (name.getMethodName(),
             EntityRegistry.class.getResourceAsStream("/OZAGREL1.json"),
             EntityRegistry.class.getResourceAsStream("/OZAGREL2.json"));
    }
}
