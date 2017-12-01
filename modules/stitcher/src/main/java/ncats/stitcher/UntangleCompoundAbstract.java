package ncats.stitcher;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import static ncats.stitcher.StitchKey.*;

public abstract class UntangleCompoundAbstract extends UntangleAbstract {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundAbstract.class.getName());

    protected UntangleCompoundAbstract (DataSource dsource) {
        super (dsource);
    }

    static protected Object getActiveMoiety (Entity e) {
        return e.payload("ActiveMoieties");
    }
    
    /*
     * this is true iff this entity is the active moiety root
     */
    static protected boolean isRoot (Entity e) {
        Object v = getActiveMoiety (e);
        if (v != null) {
            Object u = e.payload("UNII");
            return u != null && Util.delta(u, v) == null;
        }

        Entity[] out = e.outNeighbors(T_ActiveMoiety);
        Entity[] in = e.inNeighbors(T_ActiveMoiety);
        
        boolean root = false;
        if (in.length > 0 && out.length == 0) {
            // make sure in doesn't contain any active moiety (if it does)
            // then the active moiety relationship is flipped
            for (Entity nb : in) {
                v = getActiveMoiety (nb);
                if (v != null) {
                    Object u = nb.payload("UNII");
                    // can't have active moiety relationship to another entity
                    // and self, so the relationship must be flipped
                    if (u != null && Util.delta(u, v) == null)
                        return false;
                }
            }
            root = true;
        }
        
        return root;
    }

    protected boolean union (Entity... entities) {
        if (entities == null || entities.length < 2)
            return false;

        List<long[]> ids = new ArrayList<>();
        for (int i = 1; i < entities.length; ++i) {
            Entity P = entities[i-1];
            Entity Q = entities[i];
            
            Long p = uf.root(P.getId());
            Long q = uf.root(Q.getId());
            if (p != null && q != null) {
                if (!p.equals(q)) {
                    boolean pr = isRoot (ef.entity(p));
                    boolean qr = isRoot (ef.entity(q));
                    if (pr && qr) {
                        logger.warning
                            ("Can't merge two root active moieties: "
                             +P.getId()+" ("+p+" -> "
                             +Util.toString(ef.entity(p).get(I_UNII))
                             +") and "+Q.getId()+" ("+q+" ->"
                             +Util.toString(ef.entity(q).get(I_UNII))+")");
                        
                        return false; // bail out
                    }
                    else if (pr)
                        ids.add(new long[]{p, q});
                    else
                        ids.add(new long[]{q, p});
                }
            }
            else {
                Object u = getActiveMoiety (P), v = getActiveMoiety (Q);
                if (u != null && v != null && !Util.equals(u, v))
                    return false; // bail..

                ids.add(new long[]{P.getId(), Q.getId()});
            }
        }

        for (long[] pair : ids) {
            logger.info("..."+pair[0]+" ("+uf.root(pair[0])+") <-> "
                        +pair[1]+" ("+uf.root(pair[1])+")");
            uf.union(pair[0], pair[1]);
        }
        
        return !ids.isEmpty();
    }

    protected void createStitches (BiConsumer<Long, long[]> consumer) {
        // now generate untangled compoennts..
        long[][] components = uf.components();
        logger.info("There are "+components.length
                    +" components after merging!");
        for (int i = 0; i < components.length; ++i) {
            long[] comp = components[i];
            logger.info("generating component "
                        +(i+1)+"/"+components.length+"...");            
            consumer.accept(getRoot (comp), comp);
        }
    }

    /*
     * TODO: find the root active moiety and if exists return it
     */
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

                Object m = e.get(MOIETIES);
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
        
        return root != null ? root.getId() : null;
    }
}
