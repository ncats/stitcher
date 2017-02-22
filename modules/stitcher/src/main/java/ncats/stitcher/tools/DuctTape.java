package ncats.stitcher.tools;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Entity;
import ncats.stitcher.GraphDb;
import ncats.stitcher.CliqueVisitor;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;
import ncats.stitcher.Clique;
import ncats.stitcher.Component;
import ncats.stitcher.DataSource;
import ncats.stitcher.DataSourceFactory;
import ncats.stitcher.AbstractEntityVisitor;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.BuildInfo.*;

public class DuctTape implements CliqueVisitor {
    static final Logger logger = Logger.getLogger(DuctTape.class.getName());

    public static final String SOURCE = 
        "### "+DuctTape.class.getSimpleName()+" built on "
        +TIME+" (commit: " +COMMIT+"/"+BRANCH+")";

    /**
     * Don't include L3 hash
     */
    static final StitchKey[] KEYS = EnumSet.complementOf
        (EnumSet.of(StitchKey.H_LyChI_L1, StitchKey.H_LyChI_L2,
                    StitchKey.H_LyChI_L3)).toArray(new StitchKey[0]);

    static boolean DEBUG = false;
    
    class Expander extends AbstractEntityVisitor {
        public Entity start;
        StitchKey key;
        
        Expander (StitchKey key, Object value) {
            set (key, value);
            this.key = key;
        }

        @Override
        public boolean visit (Entity[] path, Entity e) {
            if (DEBUG) {
                System.out.print("  ");
                for (int i = 0; i < path.length; ++i)
                    System.out.print(" ");
                System.out.println(" + "+e.getId());
            }
            
            eqv.union(start.getId(), e.getId());
            return true;
        }
    }

    final EntityFactory ef;
    final DataSourceFactory dsf;
    List<Clique> cliques = new ArrayList<Clique>();
    UnionFind eqv = new UnionFind ();
    List<Long> singletons = new ArrayList<Long>();
    Set<Long> cnodes = new HashSet<Long>(); // clique nodes
    
    public DuctTape (GraphDb graphDb) {
        ef = new EntityFactory (graphDb);
        dsf = new DataSourceFactory (graphDb);
    }

    public void closure (String label) {
        clear ();
        
        logger.info("######### Enumerating cliques for \""+label+"\"...");
        long start = System.currentTimeMillis();
        ef.cliqueEnumeration(KEYS, label, this);
        double elapsed = (System.currentTimeMillis()-start)*1e-3;
        closure (ef.entities(label));   
        logger.info("######### Elapsed time for \""+label+"\" ("
                    +cliques.size()+" cliques) "
                    +String.format("%1$.3fs", elapsed));
        stitch (label);
    }

    public void clear () {
        cliques.clear();
        eqv.clear();
        singletons.clear();
        cnodes.clear();
    }

    public void closure () {
        // perform closure on all connected components
        int cc = 1;
        for (Iterator<Entity[]> it = ef.connectedComponents();
             it.hasNext(); ++cc) {
            Entity[] comp = it.next();
            if (comp.length > 2) {
                clear ();
                logger.info("######### Enumerating cliques for CC_"
                            +cc+" ("+comp.length+")...");
                long start = System.currentTimeMillis();
                ef.cliqueEnumeration(KEYS, comp, this);
                logger.info(cliques.size()+" clique(s) found!");
                
                closure (Arrays.asList(comp).iterator());
                double elapsed = (System.currentTimeMillis()-start)*1e-3;
                logger.info("######### Elapsed time for CC_"+cc+": "
                            +String.format("%1$.3fs", elapsed));
                stitch ("CC_"+cc);
            }
            else {
                // 
            }
        }
    }

    public void closure (Iterator<Entity> iter) {
        while (iter.hasNext()) {
            Entity e = iter.next();
            long id = e.getId();
            if (!eqv.contains(id)) {
                // node not merged, so we try to assign it to one of the
                // best available neighbors

                Long mapped = null;
                int unmapped = 0, max = 0;
                Map<Long, Integer> counts = new HashMap<Long, Integer>();
                for (Entity ne : e.neighbors()) {
                    Long r = eqv.root(ne.getId());
                    if (r != null) {
                        Integer c = counts.get(r);
                        c = c == null ? 1 : c+1;
                        counts.put(r, c);
                        if (c > max) {
                            mapped = r;
                            max = c;
                        }
                    }
                    else {
                        ++unmapped;
                        eqv.union(id, ne.getId());
                    }
                }
                
                if (unmapped > max) {
                    logger.warning("Entity "+id
                                   +" has more unmapped ("+unmapped
                                   +") neighbors than mapped ("+max+")!");
                }

                if (false && max > 0) {
                    //logger.info("** mapping entity "+id+" to "+mapped);
                    eqv.union(mapped, id);
                }
                else {
                    singletons.add(id);
                }
            }
        }
    }

    public void stitch (String name) {
        // first delete
        DataSource ds = dsf.register(SOURCE);
        ef.delete(ds);
            
        long[][] groups = eqv.components();
        System.out.println("## "+groups.length+" groups!");

        ObjectMapper mapper = new ObjectMapper ();
        ArrayNode stitches = mapper.createArrayNode();
        for (int i = 0; i < groups.length; ++i) {
            System.out.println("+++ group "+i+" ("+groups[i].length+")");
            int nc = 0;
            ArrayNode nodes = mapper.createArrayNode();
            for (int j = 0; j < groups[i].length; ++j) {
                if (cnodes.contains(groups[i][j]))
                    ++nc;
                System.out.print(" "+groups[i][j]);
                nodes.add(ef.entity(groups[i][j]).toJson());
            }
            double score = (double)nc/groups[i].length;
            
            Entity ent = ef.createStitch(ds, groups[i]);            
            ObjectNode stitch = mapper.createObjectNode();
            stitch.put("id", ent.getId());
            stitch.put("count", groups[i].length);
            stitch.put("score", score);
            stitch.put("nodes", nodes);
            stitches.add(stitch);

            System.out.println(" => "+ent.getId() + " "
                               +String.format("%1$.3f", score));
        }

        int g = groups.length;
        for (Long id : singletons) {
            if (!eqv.contains(id)) {
                Entity ent = ef.createStitch(ds, new long[]{id});
                ObjectNode stitch = mapper.createObjectNode();
                stitch.put("id", ent.getId());
                stitch.put("count", 1);
                ArrayNode nodes = mapper.createArrayNode();
                nodes.add(ef.entity(id).toJson());
                stitch.put("nodes", nodes);
                stitches.add(stitch);
                System.out.println("+++ group "+g+" (1)");
                System.out.println(" "+id+" => "+ent.getId());
                ++g;
            }
        }

        if (name != null) {
            try {
                FileWriter writer = new FileWriter (name+".json");
                mapper.writerWithDefaultPrettyPrinter()
                    .writeValue(writer, stitches);
                writer.close();
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        
        Object instances = ds.get(DataSource.INSTANCES);
        if (instances != null) {
            g += ((Number)instances).intValue();
        }
        ds.set(DataSource.INSTANCES, g);
    }

    /**
     * CliqueVisitor interface
     */
    public boolean clique (Clique clique) {
        //logger.info("Processing clique "+clique+"...");
        int index = cliques.size();

        Set<DataSource> sources = new HashSet<DataSource>();
        for (Entity e : clique.entities()) {
            sources.add(e.datasource());
        }

        Map<StitchKey, Object> values = clique.values();
        for (Map.Entry<StitchKey, Object> me : values.entrySet()) {
            switch (me.getKey()) {
            case H_LyChI_L5:
            case H_LyChI_L4:
                logger.info("Transitive closure on clique "
                            +me.getKey()+"="+me.getValue()+"...");
                closure (clique, me.getKey());
                break;

            case H_LyChI_L3:
                break;

            default:
                int count = ef.getStitchedValueCount
                    (me.getKey(), me.getValue());
                // conservative..
                int size = clique.size()*(clique.size()-1)/2;
                if (size == count && clique.size() == sources.size()) {
                    logger.info("Transitive closure on clique "
                                +me.getKey()+"="+me.getValue()+"...");
                    closure (clique, me.getKey());
                }
            }
        }
        cliques.add(clique);
        
        return true;
    }

    void closure (Clique clique, StitchKey... keys) {
        Entity[] entities = clique.entities();
        Map<StitchKey, Object> values = clique.values();
        for (StitchKey key : keys) {
            Object value = values.get(key);
            if (value != null)
                closure (key, value, entities);
        }
        
        for (Entity e : entities)
            cnodes.add(e.getId());
    }
    
    void closure (StitchKey key, Object value, Entity... entities) {
        Expander ex = new Expander (key, value);
        for (Entity e : entities) {
            if (!eqv.contains(e.getId())) {
                //System.out.println("** New path: "+key+"="+value);
                ex.start = e;
                e.walk(ex);
            }
        }
    }

    public static void main (String[] argv) throws Exception {
        System.err.println(SOURCE);
        
        if (argv.length < 1) {
            System.err.println("Usage: "+DuctTape.class.getName()
                               +" DB [LABELS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        
        try {
            /*
            EntityFactory ef = new EntityFactory (graphDb);
            Collection<Component> comps = ef.components();
            for (Component c : comps) {
                System.out.println
                    (c.getId()+" "+String.format("%1$ 5d", c.size())
                     +" "+c.nodes());
            }
            System.out.println("** "+comps.size()+" connected components!");
            */            

            DuctTape dt = new DuctTape (graphDb);
            if (argv.length > 1)
                for (int i = 1; i < argv.length; ++i)
                    dt.closure(argv[i]);
            else 
                dt.closure();
        }
        finally {
            graphDb.shutdown();
        }
    }
}
