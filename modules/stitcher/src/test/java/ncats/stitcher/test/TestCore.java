package ncats.stitcher.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.lang.reflect.Array;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import ncats.stitcher.*;
import ncats.stitcher.impl.*;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestCore extends EntityRegistry {
    static final Logger logger = Logger.getLogger(TestCore.class.getName());

    static {
        GraphDb.addShutdownHook();
    }

    static class TestPayload extends DefaultPayload {
        EntityType type;

        TestPayload (DataSource source, String name) {
            this (source, EntityType.Agent, name);
        }
        
        TestPayload (DataSource source, EntityType type, String name) {
            super (source);
            this.type = type;
            put ("name", name);
            put ("hash", name.hashCode());
            setId (name);
        }
    }

    public TestCore () throws IOException {
        super (GraphDb.createTempDb());
        setDataSource (getDataSourceFactory().register("test data source"));
    }

    Map<String, Entity> initDb1 () {
        Map<String, Entity> entities = new HashMap<String, Entity>();
        try (Transaction tx = getGraphDb().graphDb().beginTx()) {        
            Entity a = register (new TestPayload (getDataSource(), "Agent A"));
            a._add(StitchKey.I_CAS, new StitchValue (new String[]{
                        "1-2-3",
                        "2-1-3",
                        "3-1-2",
                        "3-2-1"
                    }));
            a._add(StitchKey.I_CID, new StitchValue ("CID", new int[]{
                        11, 22, 33, 44
                    }));
            a._add(StitchKey.N_Name, new StitchValue ("Name", new String[]{
                        "one", "two", "three"
                    }));
            entities.put("a", a);
            
            // connect to a through I_CAS 2-1-3 and I_CID {11, 33}
            Entity b = register (new TestPayload (getDataSource(), "Agent B"));
            b._add(StitchKey.I_CAS, new StitchValue (new String[]{
                        "1-2-4",
                        "4-2-1",
                        "2-1-3"
                    }));
            b._add(StitchKey.I_CID, new StitchValue ("CID", new int[]{
                        1, 11, 2, 3, 33, 4
                    }));
            b._add(StitchKey.N_Name, new StitchValue ("Name", new String[]{
                        "one", "four", "five"
                    }));
            entities.put("b", b);
            
            Entity c = register (new TestPayload (getDataSource(), "Agent C"));
            c._add(StitchKey.N_Name, new StitchValue (new String[]{
                        "one", "three", "four", "five"
                    }));
            entities.put("c", c);
            
            // not connecting to anything..
            Entity d = register (new TestPayload (getDataSource(), "Agent D"));
            d._add(StitchKey.I_CAS, new StitchValue (new String[]{
                        "3-2-3",
                        "5-4-3"
                    }));
            entities.put("d", d);
            tx.success();           
        }
        return entities;
    }

    public Entity register (final TestPayload payload) {
        setDataSource(payload.getSource());
        Entity ent = Entity._getEntity(_createNode ());
        return ent._add(payload);
    }

    @Test
    public void test0 () {
        Object val1 = new String[]{
            "one", "two", "three", "four", "five", "six"
        };
        Object val2 = new String[]{
            "two", "five","nine","ten"
        };
        Object val3 = "seven";

        Object obj = Util.delta(val2, val1);
        int delta = 0;
        for (int i = 0; i < Array.getLength(obj); ++i) {
            if ("nine".equals(Array.get(obj, i))
                || "ten".equals(Array.get(obj, i)))
                ++delta;
        }
        assertTrue ("1. failure -- expecting delta to be 2 but instead got "
                    +delta, delta == 2);

        obj = Util.delta(val1, val2);
        delta = 0;
        for (int i = 0; i < Array.getLength(obj); ++i) {
            if ("one".equals(Array.get(obj, i))
                || "three".equals(Array.get(obj, i))
                || "four".equals(Array.get(obj, i))
                || "six".equals(Array.get(obj, i)))
                ++delta;
        }
        assertTrue ("2. failure -- expecting delta to be 4 but instead got "
                    +delta, delta == 4);
        
        obj = Util.delta(val3, val1);
        assertTrue ("3. failure -- expecting delta to be NO_CHANGE",
                    obj == Util.NO_CHANGE);

        obj = Util.delta(val1, val1);
        assertTrue ("4. failure -- expect delta to be NULL", obj == null);
    }
    
    @Test
    public void test1 () {
        Map<String, Entity> entities = initDb1 ();
        
        Iterator<Entity> iter = find (StitchKey.I_CID, 11);
        int count = 0;
        for (; iter.hasNext(); ++count)
            iter.next();
        assertTrue ("a. failure -- there should 2 entities with I_CID = 11"
                    , count == 2);
        
        iter = find (StitchKey.I_CAS, new String[]{"1-2-100"});
        assertTrue ("b. failure -- there shouldn't be any entities "
                    +"with I_CAS = 1-2-100", !iter.hasNext());
        
        iter = find (StitchKey.N_Name, new String[]{"one", "foobar"});
        for (count = 0; iter.hasNext(); ++count)
            iter.next();
        assertTrue ("c. failure -- there should be 3 entities with N_Name=one "
                    +"but instead got "+count, count == 3);

        GraphMetrics metrics = calcGraphMetrics();
        logger.info("CurationMetric: "+Util.toJson(metrics));
        for (StitchKey key : Entity.KEYS) {
            Map<Object, Integer> dist = getStitchedValueDistribution (key);
            if (!dist.isEmpty()) {
                logger.info("## Stiched value dist for "+key);
                logger.info(Util.toJson(dist));
            }
        }

        // dump out paths
        for (StitchKey key : Entity.KEYS) {
            for (Entity e0 : entities.values()) {
                for (Entity e1 : entities.values()) {
                    Entity[] p = e0.pathTo(e1, key);
                    if (p.length > 1) {
                        logger.info("path between entity "+e0.getId()
                                    +" and "+e1.getId() + " over "+key);
                        StringBuilder sb = new StringBuilder ();
                        sb.append("["+p[0].getId()
                                  +":"+p[0].payload("name")+"]");
                        for (int i = 1; i < p.length; ++i)
                            sb.append("-["+p[i].getId()+":"
                                      +p[i].payload("name")+"]");
                        logger.info("   "+sb);
                    }
                }
            }
        }
        
        assertTrue ("1. failure -- number of connected components should be 2",
                    2 == metrics.getConnectedComponentCount());
        assertTrue ("2. failure -- number of singletons should be 1",
                    1 == metrics.getSingletonCount());
        assertTrue ("3. failure -- number of entities should be 4",
                    4 == metrics.getEntityCount());
        assertTrue ("4. failure -- number of stitches should be 9",
                    9 == metrics.getStitchCount());
        assertTrue ("5. failure -- entity 0-size distribution should be "
                    +"the same as singleton count", metrics.getSingletonCount()
                    == metrics.getEntitySizeDistribution().get(0));

        Entity a = entities.get("a");
        a.traverse((traversal, triple) -> {
                logger.info("#### "+traversal.getVisitCount()
                            +" ("+triple.source()
                            +","+triple.target()+").. "
                            +triple.values());
                return true;
            });
        
        Entity b = entities.get("b");
        assertTrue ("6. failure -- there should be a path between "
                    +"nodes a & b over N_Name = 'one'",
                    a.pathTo(b, StitchKey.N_Name, "one").length == 2);

        Entity c = entities.get("c");
        assertTrue ("7. failure -- there should be a path between "
                    +"nodes b & c over N_Name",
                    b.pathTo(c, StitchKey.N_Name).length == 2);

        assertTrue ("8. failure -- there should be a path between "
                    +"nodes a & c over N_Name = 'three'",
                    a.pathTo(c, StitchKey.N_Name, "three").length == 2);

        // globally delete N_Name = 'one'
        delete (StitchKey.N_Name, "one");
        
        // delete the stitch between nodes a & c
        a.update(StitchKey.N_Name, "three", null);
        assertTrue ("9. failture -- there should be no path "
                    +"between nodes a & c over N_Name",
                    a.pathTo(c, StitchKey.N_Name) == Entity.EMPTY_PATH);

        logger.info("Connected components...");
        int comp = 1;
        for (Iterator<Entity[]> it = connectedComponents ();
             it.hasNext(); ++comp) {
            Entity[] cc = it.next();
            StringBuilder sb = new StringBuilder ();
            sb.append("Component "+comp+": ["+cc[0].getId()
                      +"."+cc[0].payload("name")+"]");
            for (int i = 1; i < cc.length; ++i)
                sb.append(" ["+cc[i].getId()+"."+cc[i].payload("name")+"]");
            logger.info(sb.toString());
        }

        Entity d = entities.get("d");
        iter = find (StitchKey.I_CAS, "5-4-3");
        for (count = 0; iter.hasNext(); ++count)
            iter.next();
        d.delete();
        iter = find (StitchKey.I_CAS, "5-4-3");
        assertTrue ("10. failure -- deletion doesn't work",
                    count > 0 && !iter.hasNext());

        // now delete everything..
        delete (getDataSource ());
        metrics = calcGraphMetrics();
        assertTrue ("11. failure -- number of entities should "
                    +"be 0 but instead is "+metrics.getEntityCount(),
                    0 == metrics.getEntityCount());
    }

    @Test
    public void test2 () throws Exception {
        URL url = TestCore.class.getResource("/jsonDumpINN100.txt.gz");
        File file = new File (url.toURI());
        //logger.info("name:"+file.getName());
        DataSource ds = getDataSourceFactory().register(file);
        logger.info("file:"+file+" name:"+ds.getName()+" key:"+ds.getKey());
        
        final String sha1 = "21cf71cec49129eca86664c184fb111a3f2288b8";
        assertTrue ("Data source sha1 should be "+sha1,
                    sha1.startsWith(ds.getKey()));
    }

    @Test
    public void test3 () {
        Object v1 = new String[]{"QVQRDH1X2WY6-S","V6WH9AARYJAJ-M]"};
        Object v2 = "V6WH9AARYJAJ-M";
        assertTrue ("Test equality: [QVQRDH1X2WY6-S,V6WH9AARYJAJ-M] "
                    +"vs V6WH9AARYJAJ-M should be false", !Util.equals(v1, v2));
        assertTrue ("Test equality: V6WH9AARYJAJ-M "
                    +"vs [QVQRDH1X2WY6-S,V6WH9AARYJAJ-M] should be false",
                    !Util.equals(v1, v2));
        Object v3 = new String[]{"V6WH9AARYJAJ-M"};
        assertTrue ("Test equality: [V6WH9AARYJAJ-M] "
                    +"vs V6WH9AARYJAJ-M should be true",
                    Util.equals(v3, v2));
        Object v4 = new String[]{"V6WH9AARYJAJ-M]","QVQRDH1X2WY6-S"};
        assertTrue ("Test equality: [V6WH9AARYJAJ-M,QVQRDH1X2WY6-S] "
                    +"vs [QVQRDH1X2WY6-S,V6WH9AARYJAJ-M] should be true",
                    Util.equals(v4, v1));
    }
}
