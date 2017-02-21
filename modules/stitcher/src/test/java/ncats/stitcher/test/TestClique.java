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
import static org.junit.Assert.assertTrue;

public class TestClique {
    @Rule public TestName name = new TestName();
    static final File TEMPDIR = new File (".");

    static {
        //GraphDb.addShutdownHook();
    }
    
    class MyCliqueVisitor implements CliqueVisitor {
        public EnumSet<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
        public int ncliques;

        MyCliqueVisitor () {}
        
        public boolean clique (Clique clique) {
            this.keys.addAll(clique.values().keySet());
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
    
    public TestClique () {}

    @Test
    public void testClique1 () throws Exception {
        GraphDb graphDb = GraphDb.createTempDb(name.getMethodName(), TEMPDIR);
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
             * we should now have a clique of size 5 for I_CAS and another
             * one of size 4 for {I_UNII, N_Synonym, N_Name}
             */
            MyCliqueVisitor visitor = new MyCliqueVisitor ();
            reg.cliqueEnumeration(visitor);
            assertTrue ("There should only be two cliques!",
                        visitor.ncliques == 2);
            assertTrue ("Expecting "+keys.size()+" matching keys, but instead "
                        +"got "+visitor.keys.size(),
                        visitor.keys.containsAll(keys));

            ent.delete();
        }
        finally {
            graphDb.shutdown();
        }
    }
}
