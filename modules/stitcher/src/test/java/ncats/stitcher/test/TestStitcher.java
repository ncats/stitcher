package ncats.stitcher.test;

import java.util.*;
import java.io.*;
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

    @Rule public TestName name = new TestName();
    public TestStitcher () {
    }

    @Test
    public void testStitch1 () throws Exception {
        logger.info("##### "+name.getMethodName()+" #####");
        ObjectMapper mapper = new ObjectMapper ();
        JsonNode json = mapper.readTree
            (EntityRegistry.class.getResourceAsStream("/1JQS135EYN.json"));

        ArrayNode data = (ArrayNode)json.get("data");   
        assertTrue ("Expecting json to contain 6 elements in data but "
                    +"instead got "+data.size(), data.size() == 6);
        
        EntityRegistry reg = new EntityRegistry
            (GraphDb.createTempDb(name.getMethodName()));
        reg.setStrucField("MOLFILE");
        // setup mapping
        reg.add(I_UNII, "UNII");
        reg.add(N_Name, "Synonyms");
        reg.add(I_CAS, "CAS");
        reg.add(I_CID, "PUBCHEM");
        reg.add(N_Name, "CompoundName");
        reg.add(N_Name, "CompoundSynonym");
        reg.add(I_CAS, "Cas");
        reg.add(I_UNII, "CompoundUNII");
        reg.add(I_CID, "Cid");
        reg.add(T_Keyword, "Class");
        reg.add(T_Keyword, "DATASET");

        // set data source for this registry
        reg.setDataSource(reg.getDataSourceFactory()
                          .register(name.getMethodName()));

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
            Entity e = reg.register(row);
            String id = (String) row.get("id");
            if (id != null) {
                entities.put(id, e);
                Object am = row.get("ActiveMoieties");
                if (am != null && !id.equals(am))
                    activeMoieties.put(id, (String)am);
            }
        }

        for (Map.Entry<String, String> me : activeMoieties.entrySet()) {
            Entity child = entities.get(me.getKey());
            Entity parent = entities.get(me.getValue());
            child.stitch(parent, T_ActiveMoiety, me.getValue());
        }
        
        long count = reg.count(AuxNodeType.ENTITY);
        assertTrue ("Expecting "+data.size()+" entities but instead got "
                    +count, data.size() == count);

        DataSource ds = reg.getDataSourceFactory()
            .register(name.getMethodName()+"-stitch");
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
}
