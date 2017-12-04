package ncats.stitcher.test;

import java.util.*;
import java.io.*;
import java.util.zip.GZIPInputStream;
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


public class TestHairball {
    static final Logger logger =
        Logger.getLogger(TestHairball.class.getName());

    @Rule public TestName name = new TestName();
    public TestHairball () {
    }

    void loadTest (String name, Double threshold, InputStream... streams)
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
        assertTrue ("Expecting json to contain "+total+" elements in data but "
                    +"instead got "+data.size(), data.size() == total);

        TestRegistry reg = new TestRegistry (name);
        reg.register(data);

        long count = reg.count(AuxNodeType.ENTITY);
        assertTrue ("Expecting "+data.size()+" entities but instead got "
                    +count, data.size() == count);

        DataSource ds = reg.getDataSourceFactory().register("stitch_v1");
        /*
        List<Long> comps = new ArrayList<>();
        int nc = reg.components(comps);
        for (Long id : comps) {
            Component comp = reg.component(id);
            reg.untangle(new UntangleCompoundComponent (ds, comp));
        }
        */
        reg.untangle(new UntangleCompoundStitches (ds, threshold));
        reg.shutdown(); 
    }
    
    @Test
    public void testHairball1 () throws Exception {
        logger.info("##################################### "
                    +name.getMethodName());
        loadTest (name.getMethodName(), null,
                  EntityRegistry.class.getResourceAsStream
                  ("/calcium_hairball.json")
                  /* new GZIPInputStream
                      (EntityRegistry.class.getResourceAsStream
                      ("/2277.json.gz"))*/
                  );
    }
}
