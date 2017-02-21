package ix.curation;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import ix.curation.graph.UnionFind;

public class StitchUntangled {
    static final Logger logger = Logger.getLogger
        (StitchUntangled.class.getName());

    final EntityFactory ef;
    public StitchUntangled (GraphDb graphDb) {
        ef = new EntityFactory (graphDb);
    }

    public int untangle (Component component) {
        Util.dump(component);
        UnionFind uf = new UnionFind ();

        /*
        for (StitchKey k : EnumSet.allOf(StitchKey.class)) {
            Map<Object, Integer> stats = component.stats(k);
            for (Map.Entry<Object, Integer> v : stats.entrySet()) {
                if (v.getValue() < 200) {
                    component.cliques(k, v.getKey(), c -> {
                            Util.dump(c);
                            
                            long[] nodes = c.nodes();
                            for (int i = 0; i < nodes.length; ++i)
                                for (int j = i+1; j < nodes.length; ++j)
                                    uf.union(nodes[i], nodes[j]);
                            return true;
                        });
                }
            }
        }
        */

        component.cliques(c -> {
                Util.dump(c);
                
                Map<StitchKey, Object> values = c.values();
                // first only consider cliques that span multiple stitches
                boolean ok = true;//values.size() > 1;
                if (!ok) {
                    // only one stitch key
                    StitchKey key = values.keySet().iterator().next();
                    ok = key == StitchKey.H_LyChI_L4
                        || key == StitchKey.H_LyChI_L5;
                }
                
                if (ok) {
                    long[] nodes = c.nodes();
                    for (int i = 0; i < nodes.length; ++i)
                        for (int j = i+1; j < nodes.length; ++j)
                            uf.union(nodes[i], nodes[j]);
                }
                
                return true;
            }, StitchKey.keys(1, -1));

        long[][] clumps = uf.components();
        System.out.println("**** Clumps *****");
        for (int i = 0; i < clumps.length; ++i) {
            System.out.print((i+1)+" "+clumps[i].length+":");
            for (int j = 0; j < clumps[i].length; ++j)
                System.out.print(" "+clumps[i][j]);
            System.out.println();
            ef.cliqueEnumeration(clumps[i], c -> {
                    Util.dump(c);
                    return true;
                });
        }
        
        return 0;
    }

    public int untangle (long id) {
        return untangle (ef.component(id));
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "+StitchUntangled.class.getName()
                               +" DB COMPONENTS...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            StitchUntangled su = new StitchUntangled (graphDb);
            for (int i = 1; i < argv.length; ++i) {
                su.untangle(Long.parseLong(argv[i]));
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
