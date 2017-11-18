package ncats.stitcher;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.*;
import java.net.URI;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;

import ncats.stitcher.graph.UnionFind;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundStitches extends UntangleStitches {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundStitches.class.getName());

    public UntangleCompoundStitches (DataSource dsource) {
        super (dsource);
    }
    
    @Override
    protected Long getRoot (long[] comp) {
        if (comp.length == 1)
            return comp[0];

        Entity root = null;
        if (comp.length > 0) {
            Entity[] entities = ef.entities(comp);
            if (entities.length != comp.length)
                logger.warning("There are missing entities in component!");
            
            int moieties = 0;
            for (Entity e : entities) {
                Entity[] in = e.inNeighbors(T_ActiveMoiety);
                Entity[] out = e.outNeighbors(T_ActiveMoiety);
                
                if (in.length > 0 && out.length == 0) {
                    root = e;
                    break;
                }

                Object m = e.get(Props.MOIETIES);
                if (m != null) {
                    int mc = m.getClass().isArray() ? Array.getLength(m) : 1;
                    if (root == null || mc < moieties) {
                        root = e;
                        moieties = mc;
                    }
                    else if (root.getId() > e.getId())
                        root = e;
                }
                else {
                    // just pick the lower id
                    if (root == null
                        || (moieties == 0 && root.getId() > e.getId()))
                        root = e;
                }
            }
        }

        Long r = root.getId();
        if (r != null) {
            // make sure the root is an entity from gsrs source
            Entity[] e = ef.entities(new long[]{r});
            URI uri = e[0].datasource().toURI();
            if (uri != null && !uri.toString().endsWith(".gsrs"))
                r = null;
        }
        
        if (r == null) {
            for (Entity e : ef.entities(comp)) {
                URI uri = e.datasource().toURI();
                if (uri != null && uri.toString().endsWith(".gsrs")) {
                    if (r == null || r > e.getId())
                        r = e.getId();
                }
            }
        }
        
        return r != null ? r : root.getId();
    }

    @Override
    protected boolean isCompatible (Entity.Triple triple) {
        if (triple.contains(T_ActiveMoiety)
            || triple.contains(H_LyChI_L5))
            return true;
        
        if (triple.contains(H_LyChI_L4)) {
            return Util.equals(triple.source().get(H_LyChI_L4),
                               triple.target().get(H_LyChI_L4));
        }

        int check = 1;
        // if stitch is based on l3, then it must be accompanied by at least
        // two other identifiers.. 
        if (triple.contains(H_LyChI_L3))
            ++check;

        return triple.size() > check;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundStitches.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            int version = Integer.parseInt(argv[1]);
            DataSource dsource =
                ef.getDataSourceFactory().register("stitch_v"+version); 

            UntangleStitches us = new UntangleCompoundStitches (dsource);
            AtomicInteger count = new AtomicInteger ();
            us.untangle(ef, s -> {
                    logger.info("$$ "+count.incrementAndGet()
                                +" new stitch created "+s.getId());
                });
            logger.info("$$$$$$$ "+count.get()+" total stitches created!");
        }
        finally {
            graphDb.shutdown();
        }
    }
}
