package ix.curation;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.function.Consumer;
import java.util.concurrent.atomic.AtomicInteger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import ix.curation.EntityFactory;
import ix.curation.Entity;
import ix.curation.GraphDb;
import ix.curation.CliqueVisitor;
import ix.curation.StitchKey;
import ix.curation.Util;
import ix.curation.Clique;
import ix.curation.Component;
import ix.curation.DataSource;
import ix.curation.DataSourceFactory;
import ix.curation.AbstractEntityVisitor;
import ix.curation.graph.UnionFind;

public class CliqueEntityResolution implements EntityResolution, CliqueVisitor {
    static final Logger logger =
        Logger.getLogger(CliqueEntityResolution.class.getName());

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

    EntityFactory ef;
    List<Clique> cliques = new ArrayList<Clique>();
    UnionFind eqv = new UnionFind ();
    List<Long> singletons = new ArrayList<Long>();
    Set<Long> cnodes = new HashSet<Long>(); // clique nodes
    
    public CliqueEntityResolution () {
    }

    /**
     * EntityResolution interface
     */
    public void resolve (EntityFactory ef, Consumer<Entity[]> component) {
        this.ef = ef;
        ef.component(comp -> {
                clear ();               
                Entity[] entities = comp.entities();
                if (entities.length > 1) {
                    logger.info(">>>>> cliques for "
                                +"component of size "+comp.size()+"...");
                    long start = System.currentTimeMillis();
                    ef.cliqueEnumeration(KEYS, entities, this);
                    logger.info(cliques.size()+" clique(s) found!");
                    
                    closure (Arrays.asList(entities).iterator());
                    double elapsed = (System.currentTimeMillis()-start)*1e-3;
                    logger.info("<<<<< Elapsed time for clique enumeration: "
                                +String.format("%1$.3fs", elapsed));
                    
                    for (long[] group : eqv.components()) 
                        component.accept(ef.entities(group));
                    
                    for (Long id : singletons)
                        component.accept(ef.entities(new long[]{id}));
                }
                else {
                    component.accept(entities);
                }
            });
    }

    void clear () {
        cliques.clear();
        eqv.clear();
        singletons.clear();
        cnodes.clear();
    }

    void closure (Iterator<Entity> iter) { // transitive closure
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
                else if (unmapped == 0) {
                    singletons.add(id);
                }
            }
        }
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
                // conservative.. maximum span of datasource on this clique
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
        if (argv.length < 1) {
            System.err.println("Usage: "+CliqueEntityResolution.class.getName()
                               +" DB [LABELS...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            CliqueEntityResolution cer = new CliqueEntityResolution ();
            AtomicInteger total = new AtomicInteger ();
            cer.resolve(ef, comp -> {
                    //System.out.println(c.length+" component resolved");
                    System.out.println("+++++ "+total+" ("
                                       +comp.length+") +++++");
                    for (Entity e : comp) {
                        Map<StitchKey, Object> keys = e.keys();
                        System.out.print
                            (String.format("%1$10s[%2$ 10d]",
                                           e.payloadId().toString(), e.getId())
                             +": "+keys.size()+"={");
                        int i = 0;
                        for (Map.Entry<StitchKey, Object> me
                                 : keys.entrySet()) {
                            System.out.print(me.getKey()+"="
                                             +Util.toString(me.getValue()));
                            if (++i < keys.size())
                                System.out.print(" ");
                        }
                        System.out.println("}");
                    }
                    System.out.println();
                    total.incrementAndGet();
                });
            System.out.println(total+" total components!");
        }
        finally {
            graphDb.shutdown();
        }
    }
}
