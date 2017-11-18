package ncats.stitcher;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;

import ncats.stitcher.graph.UnionFind;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

public class UntangleStitches implements EntityVisitor {
    static final Logger logger = Logger.getLogger
        (UntangleStitches.class.getName());

    static final int DEBUG = Integer.getInteger("stitcher.debug", 0);
    
    final protected DataSource dsource;
    final protected UnionFind uf = new UnionFind ();

    protected EntityFactory ef;

    public UntangleStitches (DataSource dsource) {
        this.dsource = dsource;
    }

    public void untangle (EntityFactory ef, Consumer<Stitch> consumer) {
        this.ef = ef;
        uf.clear();
        
        ef.traverse(this);
        long[][] components = uf.components();
        
        logger.info("!!!!!! "+components.length+" components untangled");
        for (int i = 0; i < components.length; ++i) {
            long[] comp = components[i];
            logger.info("#### creating stitch "+(i+1)
                        +"/"+components.length+"...");
            
            Component component = ef.component(getRoot (comp), comp);
            Stitch stitch = ef.createStitch(dsource, component);
            if (consumer != null)
                consumer.accept(stitch);
        }
    }

    /*
     * override by subclass to provide more sensible default
     */
    protected Long getRoot (long[] comp) {
        return comp.length > 0 ? comp[0] : null;
    }

    int old = 0;
    Entity.Traversal traversal = null;
    public boolean visit (Entity.Traversal traversal, Entity.Triple triple) {
        if (this.traversal != traversal)
            old = 0;
        this.traversal = traversal;
        
        boolean compatible = isCompatible (triple);
        if (compatible) {
            long c = uf.union(triple.source().getId(),
                              triple.target().getId());
            if (DEBUG > 0) {
                String s = triple.source().getId()
                    +(uf.root(triple.source().getId()) != null ?
                      ":"+uf.root(triple.source().getId()) :"");
                String t = triple.target().getId()
                    +(uf.root(triple.target().getId()) != null ?
                      ":"+uf.root(triple.target().getId()) :"");
                logger.info("$$ ("+s+","+t+") => "+c+"\n"+triple.values());
            }
        }
        else {
            if (DEBUG > 0) {
                logger.info("!! ("+triple.source().getId()+","
                            +triple.target().getId()
                            +") "+triple.values());
            }
        }

        int pct = (int)(traversal.getVisitCount()*100.
                        / traversal.getRank()+.5);
        if (pct >= (old+5)) {
            logger.info(String.format("%1$2d%% %2$5d",
                                      pct, traversal.getVisitCount())
                        +": ("+triple.source().getId()+","
                        +triple.target().getId()+") "+triple.values());
            old = pct;
        }
        
        return true;
    }

    /*
     * customization should be done through subclass
     */
    protected boolean isCompatible (Entity.Triple triple) {
        return triple.values().size() > 1;
    }
}
