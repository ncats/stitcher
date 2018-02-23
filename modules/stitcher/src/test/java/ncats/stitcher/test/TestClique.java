package ncats.stitcher.test;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;

import ncats.stitcher.Entity;
import ncats.stitcher.GraphDb;
import ncats.stitcher.EntityFactory;
import ncats.stitcher.EntityRegistry;
import ncats.stitcher.StitchKey;
import ncats.stitcher.DataSource;
import ncats.stitcher.CliqueVisitor;
import ncats.stitcher.Clique;
import ncats.stitcher.impl.MapEntityFactory;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TestName;
import static org.junit.Assert.*;

public class TestClique {
    @Rule public TestName name = new TestName();
    static final File TEMPDIR = new File (".");

    static {
        //GraphDb.addShutdownHook();
    }
    
    class MyCliqueVisitor implements CliqueVisitor {
        public EnumSet<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
        public int ncliques;

        public List<Set<StitchKey>> cliquesByKey = new ArrayList<>();
        MyCliqueVisitor () {}
        
        public boolean clique (Clique clique) {
            Set<StitchKey> stitchKeys = clique.values().keySet();
            cliquesByKey.add(stitchKeys);

            this.keys.addAll(stitchKeys);
            System.out.println("+++++++ "+String.format("%1$5d.", ++ncliques)
                               +" Clique ("+clique.size()+") ++++++++");
            System.out.println("## values: "+clique.values());
            for (Entity e : clique.entities()) {
                System.out.println
                    (String.format("%1$10d", e.getId())+" "+e.datasource());
            }
            return true;
        }
    }


    @Test
    public void testClique1 () throws Exception {
        GraphDb graphDb = GraphDb.createTempDb(name.getMethodName());
        try {
            MapEntityFactory reg = new MapEntityFactory (graphDb);
            DataSource source = reg.getDataSourceFactory()
                .register(name.getMethodName());
            reg.setDataSource(source);
            
            EnumSet<StitchKey> keys = EnumSet.of
                (StitchKey.I_CAS,
                 StitchKey.I_UNII, StitchKey.N_Name);
            for (StitchKey k : keys)
                reg.add(k, k.name());

            Entity ent = null;
            for (int i = 0; i < keys.size(); ++i) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (StitchKey k : keys)
                    map.put(k.name(), k+"-value");
                Entity e = reg.register(map);
                if (ent == null)
                    ent = e;
            }

            Map<String, Object> map = new HashMap<String, Object>();
            map.put(StitchKey.I_CAS.name(), StitchKey.I_CAS+"-value");
            reg.register(map);

            /*
             * we should now have a clique of size 4 for I_CAS and another
             * one of size 3 for {I_UNII, N_Name}
             */
            MyCliqueVisitor visitor = new MyCliqueVisitor ();
            reg.cliques(visitor);
            assertEquals ("There should only be two cliques!", 2,
                        visitor.ncliques);
            assertTrue ("Expecting "+keys.size()+" matching keys, but instead "
                    +"got "+visitor.keys.size(),
                    visitor.keys.containsAll(keys));

            assertEquals(2, visitor.cliquesByKey.size());
            assertEquals(EnumSet.of(StitchKey.N_Name, StitchKey.I_UNII), visitor.cliquesByKey.get(0));
            assertEquals(EnumSet.of(StitchKey.I_CAS), visitor.cliquesByKey.get(1));


            ent.delete();
        }
        finally {
            graphDb.shutdown();
        }
    }
}
