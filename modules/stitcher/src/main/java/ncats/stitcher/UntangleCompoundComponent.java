package ncats.stitcher;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.neo4j.graphdb.GraphDatabaseService;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundComponent {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundComponent.class.getName());

    final Component component;
    final UnionFind uf = new UnionFind ();
    final Map<StitchKey, Map<Object, Integer>> stats;
    final Map<Object, Entity> moieties = new HashMap<>();
    
    public UntangleCompoundComponent (Component component) {
        stats = new HashMap<>();
        for (StitchKey key : Entity.KEYS) {
            stats.put(key, component.stats(key));
        }
        this.component = component;
    }

    static String toString (Object value, char sep) {
        StringBuilder sb = new StringBuilder ();
        if (value.getClass().isArray()) {
            for (int i = 0; i < Array.getLength(value); ++i) {
                if (sb.length() > 0)
                    sb.append(sep);
                sb.append(Array.get(value, i).toString());
            }
        }
        else {
            sb.append(value.toString());
        }

        return sep == ',' ? "["+sb+"]" : sb.toString();
    }

    public void untangle () {
        System.out.println("## Active moieties for component ##");
        System.out.println(component.nodeSet());

        // collapse based on single/terminal active moieties
        component.stitches((source, target) -> {
                Entity[] out = source.outNeighbors(T_ActiveMoiety);
                Entity[] in = target.inNeighbors(T_ActiveMoiety);
                System.out.println(" ("+out.length+") "+source.getId()
                                   +" -> "+target.getId()+" ("+in.length+")");
                if (out.length == 1) {
                    // first collapse single/terminal active moieties
                    uf.union(source.getId(), target.getId());
                }

                for (Entity e : in) {
                    Object value = e.get(H_LyChI_L4.name());
                    if (value != null) {
                        String key = toString (value, ':');
                        Entity old = moieties.put(key, e);
                        if (old != null) {
                            logger.warning("** "+H_LyChI_L4+"="+key+" maps to "
                                           +"active moieties "+e.getId()
                                           +" and "+old.getId()+" **");
                        }
                    }
                    else {
                        logger.warning("** No "+H_LyChI_L4
                                       +" value for "+e.getId());
                    }
                }
                
                Object value = target.get(H_LyChI_L4.name());
                if (value != null) {
                    String key = toString (value, ':');
                    moieties.put(key, target);
                }
            }, T_ActiveMoiety);
        dump ("Active moiety stitching");

        // collapse based on lychi layer 5
        component.stitches((source, target) -> {
                uf.union(source.getId(), target.getId());
            }, H_LyChI_L5);
        dump (H_LyChI_L5+" stitching");
        
        // now find all unmapped nodes
        int count = 0;
        for (Entity e : component) {
            if (!uf.contains(e.getId())) {
                Entity[] nb = e.outNeighbors(T_ActiveMoiety);
                if (nb.length > 1) {
                    if (!union (e, nb, T_ActiveMoiety)) {
                        logger.warning("** unmapped entity "+e.getId()+": "
                                       +e.keys());
                        ++count;
                    }
                }
                // try lychi layer 4
                else if (nb.length == 0
                         && !union (e, e.neighbors(H_LyChI_L4), H_LyChI_L4)) {
                    logger.warning("** unmapped entity "+e.getId()+": "
                                   +e.keys());
                    ++count;
                }
            }
        }
        dump ("unmapped nodes");
        
        logger.info("### "+count+" unstitched entities!");
    }

    boolean union (Entity e, Entity[] nb, StitchKey key) {
        Entity emin = null;
        Object vmin = null;
        Integer cmin = null;
        
        for (Entity u : nb) {
            Map<StitchKey, Object> keys = e.keys(u);
            
            Object value = keys.get(key);
            if (value != null && value.getClass().isArray()) {
                int len = Array.getLength(value);
                if (len == 0) {
                    value = null;
                }
                else if (len > 1) { // multiple values stitched
                    for (int i = 0; i < len; ++i) {
                        Object v = Array.get(value, i);
                        if (moieties.containsKey(v)) {
                            if (vmin == null || !moieties.containsKey(vmin)
                                || (stats.get(key).get(v)
                                    < stats.get(key).get(vmin))) {
                                vmin = v;
                                emin = u;
                                cmin = 0;
                            }
                        }
                        /*
                        else {
                            Integer c = stats.get(key).get(v);
                            if (cmin == null || cmin > c) {
                                emin = u;
                                cmin = c;
                                vmin = v;
                            }
                        }
                        */
                    }
                    value = null;
                }
                else {
                    value = Array.get(value, 0);
                }
            }

            if (value != null) {
                Integer c = stats.get(key).get(value);
                if (c == null) {
                    logger.warning("** entity "+e.getId()
                                   +" "+key+"="+value
                                   +" has no count! ** ");
                }
                else if (cmin == null || cmin > c) {
                    emin = u;
                    cmin = c;
                    vmin = value;
                }
            }
        }
        
        // now merge
        boolean ok = false;
        if (emin != null) {
            if (uf.contains(emin.getId())) {
                if (cmin < 1000) {
                    logger.info(".."+e.getId()+" <-["+key+"="
                                +vmin+":"+cmin+"]-> "+emin.getId());
                    uf.union(e.getId(), emin.getId());
                    ok = true;
                }
            }
            else {
                logger.info(".."+e.getId()+" <-["+key+"="
                            +vmin+":"+cmin+"]-> "+emin.getId());
                uf.union(e.getId(), emin.getId());
                ok = true;
            }
        }
        
        return ok;
    }

    void dump (String mesg) {
        long[][] components = uf.components();
        System.out.println("** "+mesg+": number of components: "
                           +components.length);
        for (long[] c : components) {
            System.out.print(c.length+" [");
            for (int i = 0; i < c.length; ++i) {
                System.out.print(c[i]);
                if (i+1 < c.length)
                    System.out.print(",");
            }
            System.out.println("]");
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundComponent.class.getName()
                               +" DB COMPONENTS...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);         
            for (int i = 1; i < argv.length; ++i) {
                Component comp = ef.component(Long.parseLong(argv[i]));
                logger.info("Dumping component "+comp.getId());         
                FileOutputStream fos = new FileOutputStream
                    ("Component_"+comp.getId()+".txt");
                Util.dump(fos, comp);
                fos.close();
                
                logger.info("Stitching component "+comp.getId());
                UntangleCompoundComponent ucc =
                    new UntangleCompoundComponent (comp);
                ucc.untangle();
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
