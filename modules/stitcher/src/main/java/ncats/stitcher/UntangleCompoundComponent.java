package ncats.stitcher;

import java.util.*;
import java.io.*;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;
import java.util.function.BiConsumer;

import chemaxon.struc.Molecule;

import org.neo4j.graphdb.GraphDatabaseService;
import static ncats.stitcher.StitchKey.*;

public class UntangleCompoundComponent extends UntangleCompoundAbstract {
    static final Logger logger = Logger.getLogger
        (UntangleCompoundComponent.class.getName());

    final Map<Object, Set<Entity>> moieties = new TreeMap<>();
    final protected Component component;
    final protected Map<StitchKey, Map<Object, Integer>> stats;

    class TransitiveClosure {
        Entity emin;
        Object vmin;
        Integer cmin;
        
        final Map<Object, Integer> counts;
        final Entity e;
        final Entity[] nb;
        final StitchKey key;
        final Object kval;
        
        TransitiveClosure (Entity e, StitchKey key) {
            this (e, e.neighbors(key), key);
        }
        
        TransitiveClosure (Entity e, Entity[] nb, StitchKey key) {
            this.e = e;
            this.nb = nb;
            this.key = key;
            
            kval = e.get(key);
            counts = stats.get(key);
        }

        boolean checkNoExt (Object value, Set except, String ext) {
            Set set = Util.toSet(value);
            if (except != null)
                set.removeAll(except);
            
            for (Object sv : set) {
                if (!sv.toString().endsWith(ext))
                    return false;
            }
            return true;
        }

        boolean checkExt (Object value, String ext) {
            Set set = Util.toSet(value);
            for (Object sv : set) {
                if (sv.toString().endsWith(ext))
                    return true;
            }
            return false;
        }

        boolean checkActiveMoiety (Entity u, Object value) {
            Set<Entity> su = moieties.get
                (valueToString (u.get(H_LyChI_L4), ':'));
            if (su != null) {
                Set<Entity> v = moieties.get(valueToString (value, ':'));
                if (v != null)
                    for (Entity z : v)
                        if (su.contains(z))
                            return true;
                
                // try individual values?
                if (value.getClass().isArray()) {
                    for (int i = 0; i < Array.getLength(value); ++i) {
                        v = moieties.get
                            (valueToString(Array.get(value, i), ':'));
                        if (v != null && su.containsAll(v))
                            return true;
                    }
                }
            }
            
            return su == null;
        }

        void updateIfCompatible (Entity u, Object v) {
            Integer c = null;
            if (v.getClass().isArray()) {
                for (int i = 0; i < Array.getLength(v); ++i) {
                    Integer n = counts.get(Array.get(v, i));
                    if (c == null || n < c)
                        c = n;
                }
            }
            else
                c = counts.get(v);

            boolean hasMetal = false;
            Set vset = Util.toSet(v);
            for (Object sv : vset) {
                if (sv.toString().endsWith("-M")) {
                    hasMetal = true;
                    break;
                }
            }
            
            if (cmin == null
                || (cmin > c && !checkExt (vmin, "-M")) || hasMetal) {
                boolean update = checkNoExt (u.get(key), vset, "-S")
                    && checkNoExt (kval, vset, "-S")
                    && checkActiveMoiety (u, v);
                
                if (update) {
                    emin = u;
                    cmin = c;
                    vmin = v;
                }
            }
        }
        
        public boolean closure () {
            for (Entity u : nb) {
                Object value = e.keys(u).get(key);
                if (value != null) {
                    updateIfCompatible (u, value);
                }
            }
        
            // now merge
            boolean ok = false;
            if (emin != null) {
                if (uf.contains(emin.getId())) {
                    if (cmin < 1000) {
                        logger.info(".."+e.getId()+" <-["+key+"="
                                    +vmin+":"+cmin+"]-> "+emin.getId());
                        union (e, emin);
                        ok = true;
                    }
                }
                else {
                    logger.info(".."+e.getId()+" <-["+key+"="
                                +vmin+":"+cmin+"]-> "+emin.getId());
                    union (e, emin);
                    ok = true;
                }
            }
            
            return ok;
        } // closure
    } // TransitiveClosure

    class CliqueClosure {
        final Entity entity;
        final StitchKey[] span;
        final Map<StitchKey, Object> values;
        final boolean anyvalue; // single valued?

        Map<Clique, Set<StitchKey>> cliques = new HashMap<>();
        Clique bestClique; // best clique so far
        Map<Long, Integer> nodes = new HashMap<>();

        CliqueClosure (Entity entity, StitchKey... span) {
            this (entity, true, span);
        }
        
        CliqueClosure (Entity entity, boolean anyvalue, StitchKey... span) {
            this.entity = entity;
            this.span = span;
            this.anyvalue = anyvalue;
            values = entity.keys();
        }

        CliqueClosure (StitchKey key, Object value, StitchKey... span) {
            values = new EnumMap<>(StitchKey.class);
            values.put(key, value);
            entity = null;
            anyvalue = false;
            this.span = span;
        }

        public boolean closure () {
            for (StitchKey k : span) {
                Object value = values.get(k);
                if (value != null) {
                    if (entity != null) {
                        logger.info("***** searching for clique (e="
                                    +entity.getId()+") "
                                    +k+"="+Util.toString(value));
                    }
                    else {
                        logger.info("***** searching for clique "
                                    +k+"="+Util.toString(value));
                    }
                    
                    if (value.getClass().isArray()) {
                        int len = Array.getLength(value);
                        if (anyvalue || len == 1) {
                            for (int i = 0; i < len; ++i) {
                                Object val = Array.get(value, i);
                                findClique (k, val);
                            }
                        }
                    }
                    else {
                        findClique (k, value);
                    }
                }
            }

            // now merge all nodes in the clique!
            boolean closure = false;
            if (bestClique != null) {
                Entity[] entities = bestClique.entities();
                for (int i = 1; i < entities.length; ++i)
                    union (entities[i], entities[i-1]);
                closure = true;
            }
            else if (!nodes.isEmpty()) {
                // find labeled nodes that maximally span multiple cliques
                logger.info("No best clique found, so resort to "
                            +"clique span heuristic.");

                for (Map.Entry<Clique, Set<StitchKey>> me :
                         cliques.entrySet()) {
                    Clique clique = me.getKey();
                    Set<StitchKey> keys = me.getValue();
                    if (keys.size() > 1) {
                        logger.info("## collapsing clique "+clique.nodeSet()
                                    +" due to multiple spans: "+keys);
                        for (Entity e : clique.entities())
                            union (entity, e);
                        closure = true;
                    }
                }
            }

            return closure;
        }

        boolean containsExactly (Component comp, StitchKey key, Object value) {
            for (Entity e : comp) {
                Object dif = Util.delta(e.get(key), value);
                if (dif == Util.NO_CHANGE) {
                    return false;
                }
                else if (dif != null) {
                    switch (key) {
                    case H_LyChI_L3:
                    case H_LyChI_L4:
                    case H_LyChI_L5:
                        // if the remaining entries are salt/solvent, then
                        // it's still ok?
                        for (int i = 0; i < Array.getLength(dif); ++i) {
                            String v = (String) Array.get(dif, i);
                            if (v.endsWith("-N") || v.endsWith("-M"))
                                return false;
                        }
                        break;
                        
                    default:
                        return false;
                    }
                }
            }
            return true;
        }

        void findClique (StitchKey key, Object value) {
            Integer count = stats.get(key).get(value);
            component.cliques(clique -> {
                    logger.info("$$ found clique "+key+"="
                                +Util.toString(value)
                                +" ("+count+") => ");
                    Util.dump(clique);
                    
                    if (count != (clique.size() * (clique.size() -1)/2)) {
                        logger.warning("** might be spurious clique **");
                        return true;
                    }
                    
                    // collapse components
                    if ((entity != null && clique.contains(entity))
                        || entity == null) {
                        Map<Long, Integer> classes = new HashMap<>();
                        int unmapped = 0;
                        for (Long n : clique.nodeSet()) {
                            Long c = uf.root(n);
                            logger.info(" .. "+n+" => "+ c);
                            if (c != null) {
                                Integer cnt = classes.get(c);
                                classes.put(c, cnt == null ? 1 :cnt+1);
                                // keeping track of labeled nodes in cliques
                                cnt = nodes.get(n);
                                nodes.put(n, cnt == null ? 1 : cnt+1);
                            }
                            else
                                ++unmapped;
                        }

                        if (classes.size() == 1) {
                            // nothing to do
                        }
                        else if (classes.size() < 2
                            && (anyvalue
                                || containsExactly (clique, key, value))
                            && (bestClique == null
                                || bestClique.score() < clique.score())) {//bestClique.size() < clique.size())) {
                            bestClique = clique;
                            logger.info("## best clique updated: "
                                        +clique.getId()+" key="+key
                                        +" value="+Util.toString(value));
                        }
                        else {
                            // find larger cliques that contains the same
                            //  set of members and update their keys
                            updateCliques (cliques, key, clique);
                        }
                    }
            
                    return true;
                }, key, value);
        }
    } // CliqueClosure
    
    public UntangleCompoundComponent (DataSource dsource,
                                      Component component) {
        super (dsource);
        
        stats = new HashMap<>();
        for (StitchKey key : Entity.KEYS) {
            stats.put(key, component.stats(key));
        }
        
        long created = (Long) dsource.get(CREATED);
        for (Entity e : component) {
            long c = (Long) e.get(CREATED);
            if (c > created) {
                throw new IllegalArgumentException
                    ("Can't use DataSource \""+dsource.getName()+"\" ("
                     +dsource.getKey()+") "
                     +"as reference to untangle component "
                     +component.getId()+" because data has changed "
                     +"since it was last created!");
            }
        }
        
        this.component = component;
    }

    static void updateCliques (Map<Clique, Set<StitchKey>> cliques,
                               StitchKey key, Clique clique) {
        List<Clique> superCliques = new ArrayList<>();
        List<Clique> subCliques = new ArrayList<>();    
        for (Map.Entry<Clique, Set<StitchKey>> me
                 : cliques.entrySet()) {
            Clique c = me.getKey();
            if (c.size() > clique.size()) {
                Component ov = c.and(clique);
                if (ov != null && ov.equals(clique))
                    superCliques.add(c);
            }
            else if (c.size() < clique.size()) {
                Component ov = clique.and(c);
                if (ov != null && ov.equals(c))
                    subCliques.add(c);
            }
        }
                            
        Set<StitchKey> set = cliques.get(clique);
        if (set == null) {
            cliques.put(clique, set = EnumSet.noneOf(StitchKey.class));
        }
                            
        set.add(key);
        // merge other keys into this clique
        for (Clique c : superCliques)
            set.addAll(cliques.get(c));
        for (Clique c : subCliques)
            cliques.get(c).add(key);
    }


    public void untangle (EntityFactory ef,
                          BiConsumer<Long, long[]> consumer) {
        logger.info("####### Untangling component: "+component);
        this.ef = ef;

        // collapse based on single/terminal active moieties
        Set<Entity> unsure = new TreeSet<>();
        component.stitches((source, target) -> {
                Entity[] out = source.outNeighbors(T_ActiveMoiety);
                Entity[] in = target.inNeighbors(T_ActiveMoiety);
                logger.info(" ("+out.length+") "+source.getId()
                            +" -> "+target.getId()+" ["
                            +isRoot (target)+"] ("+in.length+")");
                
                if (out.length == 1) {
                    // first collapse single/terminal active moieties
                    uf.union(target.getId(), source.getId());
                }
                else if (out.length > 1) {
                    unsure.add(source);
                }

                for (Entity e : in) {
                    Object value = e.get(H_LyChI_L4.name());
                    if (value != null) {
                        String key = valueToString (value, ':');
                        Set<Entity> set = moieties.get(key);
                        if (set == null)
                            moieties.put(key, set = new TreeSet<>());
                        set.add(target);
                    }
                    else {
                        logger.warning("** No "+H_LyChI_L4
                                       +" value for "+e.getId());
                    }
                }
                
                Object value = target.get(H_LyChI_L4.name());
                if (value != null) {
                    String key = valueToString (value, ':');
                    Set<Entity> set = moieties.get(key);
                    if (set == null)
                        moieties.put(key, set = new TreeSet<>());
                    set.add(target);
                }
            }, T_ActiveMoiety);
        //dumpActiveMoieties ();
        dump ("##### active moiety stitching");
        
        // collapse based on trusted stitch keys; e.g., unii
        component.stitches((source, target) -> {
            union (source, target);
            logger.info(source.getId() +" <-> "+target.getId()
                    +" I_UNII="
                    +Util.toString(source.get(I_UNII)));
            }, I_UNII);
        dump ("##### UNII key stitching");

//        // collapse based on trusted stitch keys; e.g., lychi layer 5, unii
//        component.stitches((source, target) -> {
//                union (source, target);
//                logger.info(source.getId() +" <-> "+target.getId()
//                            +" H_LyChI_5="
//                            +Util.toString(source.get(H_LyChI_L5)));
//            }, H_LyChI_L5);
//        dump ("##### L5 keys stitching");

        component.stitches((source, target) -> {
                Long s = uf.root(source.getId());
                Long t = uf.root(target.getId());
                if (s != null && t != null && !s.equals(t)) {
                    Object sv = source.get(H_LyChI_L4);
                    Object tv = target.get(H_LyChI_L4);
                    if (sv != null && tv != null && Util.equals(sv, tv)
                        // don't consider this if we're dealing with metals
                        /*&& !Util.contains(source.get(H_LyChI_L5), h -> {
                                return h.toString().endsWith("-M");
                            })
                        && !Util.contains(target.get(H_LyChI_L5), h -> {
                                return h.toString().endsWith("-M");
                                })*/) {
                        logger.info(source.getId() +" <-> "+target.getId()
                                    +" H_LyChI_L4="+Util.toString(tv));
                        union (source, target);
                    }
                }
            }, H_LyChI_L4);
        dump ("##### L4 keys stitching");

        // we need to do another pass over each component to see
        // if we have multi-value cliques that span components
        mergeComponents (N_Name, I_CAS, I_DB, I_ChEMBL,
                         I_CID, I_SID, H_LyChI_L4);
        //mergeCliqueComponents (H_LyChI_L3);

        List<Entity> singletons = new ArrayList<>();
        // now find all remaining unmapped nodes
        int processed = 0, total = component.size();
        for (Entity e : component) {
            if (unsure.contains(e)) {
            }
            else if (!uf.contains(e.getId())) {
                logger.info("++++++++++++++ resolving unresolved " +processed
                            +"/"+total+" ++++++++++++++");
                if (transitive (e, H_LyChI_L4)) {
                    // 
                }
                else if (clique (e, N_Name, I_CAS,
                                 I_DB, I_ChEMBL, H_LyChI_L3)) {
                }
                /*
                else if (clique (e, false, H_LyChI_L3)) {
                    // desparate, last resort but require large min clique
                }
                */
                else {
                    logger.warning("** unmapped entity "+e.getId()+": "
                                   +e.keys());
                    singletons.add(e);
                }
                ++processed;
            }
        }
        dump ("##### number of unmapped nodes: "+singletons.size());

        // now merge singeltons
        mergeSingletons (singletons, N_Name, I_CAS, I_DB, I_ChEMBL);
        
        // now handle unresolved nodes with multiple active moieties and
        // assign to the class with less references 
        for (Entity e : unsure) {
            Entity[] nb = e.outNeighbors(T_ActiveMoiety);
            if (nb.length > 1 && !uf.contains(e.getId())) {
                Map<Long, Integer> votes = new HashMap<>();
                for (Entity u : nb) {
                    for (Object v : Util.toSet(u.get(H_LyChI_L4))) {
                        if (v.toString().endsWith("-M")
                            || v.toString().endsWith("-N")) {
                            Integer c = votes.get(u.getId());
                            votes.put(u.getId(), c==null?1:c+1);
                        }
                    }
                }

                if (votes.size() == 1) {
                    Long id = votes.keySet().iterator().next();
                    uf.union(e.getId(), id);
                }
                else {
                    uf.add(e.getId()); // its own component
                }
            }
        }
        dump ("##### nodes with multiple active moieties");

        createStitches (consumer);
    }
    

    boolean transitive (Entity e, StitchKey key) {
        return new TransitiveClosure(e, key).closure();
    }

    boolean transitive (Entity e, Entity[] nb, StitchKey key) {
        return new TransitiveClosure(e, nb, key).closure();
    }

    boolean clique (Entity e, StitchKey... keys) {
        return new CliqueClosure(e, keys).closure();
    }
    
    boolean clique (Entity e, boolean anyvalue, StitchKey... keys) {
        return new CliqueClosure(e, anyvalue, keys).closure();
    }

    static boolean checkH4 (Object value) {
        boolean ok = false;
        if (value != null) {
            if (value.getClass().isArray()) {
                int len = Array.getLength(value), n = 0;
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(value, i);
                    if (v.toString().endsWith("-N"))
                        ++n;
                }
                ok = n > 0;
            }
            else {
                ok = value.toString().endsWith("-N");
            }
        }
        return ok;
    }
    
    void mergeCliqueComponents (StitchKey key) {
        Map<Object, Integer> values = component.values(key);
        logger.info("$$$$ Merging components based on spanning cliques..."
                    +key+"="+values);   
        for (Map.Entry<Object, Integer> me : values.entrySet()) {
            component.cliques(clique -> {
                    Util.dump(clique);
                    
                    Entity[] entities = clique.entities();
                    for (int i = 0; i < entities.length; ++i) {
                        long ei = entities[i].getId();
                        Long ci = uf.root(ei);
                        for (int j = i+1; j < entities.length; ++j) {
                            long ej = entities[j].getId();
                            Long cj = uf.root(ej);

                            Map<StitchKey, Object> stitches =
                                entities[i].keys(entities[j]);
                            //stitches.remove(key);
                            Object h4 = stitches.get(H_LyChI_L4);
                            if (stitches.size() > 2 /* key + another */
                                || stitches.containsKey(H_LyChI_L5)
                                || checkH4 (h4)) {
                                logger.info
                                    (ei+" <-"+stitches+"-> "+ej);
                                union (entities[i], entities[j]);
                            }
                            else {
                                logger.info("..."+ei+" ("+ci+") :: "
                                            +stitches+" :: "+ ej
                                            +" ("+cj+")");
                            }
                        }
                    }
                    
                    return true;
                }, key, me.getKey());
        }
    }
    
    void mergeComponents (StitchKey... span) {
        logger.info("Merging components with different equivalence classes...");
        final Map<Clique, Set<StitchKey>> cliques = new HashMap<>();
        for (StitchKey key : span) {
            Map<Object, Integer> values = component.values(key);
            
            boolean skip = false;
            switch (key) {
            case H_LyChI_L3:
            case H_LyChI_L4:
            case H_LyChI_L5:
                // we won't consider metals here
                for (Map.Entry<Object, Integer> me : values.entrySet()) {
                    if (me.getKey().toString().endsWith("-M")) {
                        skip = true;
                        break;
                    }
                }
                break;
            }
            
            // only consider clique of side 3 or larger             
            if (!skip) {
                for (Map.Entry<Object, Integer> me : values.entrySet()) {
                    Object value = me.getKey();
                    if (me.getValue() > 2) {
                        component.cliques(clique -> {
                                logger.info("$$$$ searching for cliques "+key
                                            +": "+me.getKey()
                                            +"="+me.getValue());
                                // only consider this if there are missing
                                // stereocenters
                                boolean cont = true;
                                if (key == H_LyChI_L3) {
                                    int undef = 0;
                                    for (Entity e : clique) {
                                        Molecule mol = e.mol();
                                        if (mol != null) {
                                            Map props = Util.calcMolProps(mol);
                                            Integer cnt = (Integer)props
                                                .get("undefinedStereo");
                                            if (cnt > 0) ++undef; 
                                        }
                                    }
                                    
                                    logger.info("$$$$ entities with undefined "
                                                +"stereocenters: "+undef);
                                    cont = undef > 0;
                                }
                                
                                if (cont) {
                                    Util.dump(clique);
                                    updateCliques (cliques, key, clique);
                                }
                                return cont;
                            }, key, value);
                    }
                } // foreach values
            } // endif cont
        }

        for (Map.Entry<Clique, Set<StitchKey>> me : cliques.entrySet()) {
            Set<StitchKey> keys = me.getValue();
            if (keys.size() > 1) {
                Clique clique = me.getKey();
                logger.info("$$$$ collapsing clique "
                            +clique.nodeSet()+" due to spanning keys "+keys);
                Entity[] entities = clique.entities();
                union (entities);
            }
        }
    }

    void mergeSingletons (Collection<Entity> singletons, StitchKey... span) {
        logger.info("Merging singletons..."+singletons.size());
        for (Entity e : singletons) {
            Map<StitchKey, Long> classes = new HashMap<>();
            for (StitchKey key : span) {
                Entity[] nb = e.neighbors(key);
                for (int i = 0; i < nb.length; ++i) {
                    Long cls = uf.root(nb[i].getId());
                    if (cls != null) {
                        Long c = classes.get(key);
                        if (c == null)
                            classes.put(key, cls);
                        else if (c == -1l) {
                        }
                        else if (!cls.equals(c)) {
                            classes.put(key, -1l);
                        }
                    }
                }
            }

            boolean merged = false;
            // previously: only merge if this entity has multiple key span
            if (classes.size() > 0) { // || classes.containsKey(StitchKey.I_UNII)) { // todo should we require unanimous?
                Map.Entry<StitchKey, Long> max = Collections.max
                    (classes.entrySet(), (a, b) -> {
                        if (b.getValue() > a.getValue()) return 1;
                        if (b.getValue() < a.getValue()) return -1;
                        return 0;
                    });
                
                if (max.getValue() > 0l) {
                    logger.info("$$$$ collapsing entity "+e.getId()
                                +" => "+max.getKey()+" ("+max.getValue()
                                +") from "+classes);
                    uf.union(max.getValue(), e.getId());
                    merged = true;
                }
            }

            if (!merged) {
                logger.info("**** singleton entity "+e.getId()+": "+e.keys());
                uf.add(e.getId());
            }
        }
    }

    void dumpActiveMoieties () {
        try {
            FileOutputStream fos = new FileOutputStream
                ("Component_"+component.getId()+"_activemoieties.txt");
            PrintStream ps = new PrintStream (fos);
            for (Map.Entry<Object, Set<Entity>> me : moieties.entrySet()) {
                ps.print(me.getKey());
                for (Entity e : me.getValue())
                    ps.print(" "+e.getId());
                ps.println();
            }
            ps.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    static void dumpComponents (EntityFactory ef) throws IOException {
        PrintStream ps = new PrintStream
            (new FileOutputStream ("components.txt"));
        Map<Integer, Integer> hist = new TreeMap<>();
        ef.components(component -> {
                Entity root = component.root();
                Integer rank = (Integer) root.get(Props.RANK);
                ps.println(root.getId()+"\t"+rank);
                Integer c = hist.get(rank);
                hist.put(rank, c == null ? 1 : c+1);
            });
        ps.close();
        
        System.out.println("Component rank histogram:");
        for (Map.Entry<Integer, Integer> me : hist.entrySet()) {
            System.out.println(me.getKey()+"\t"+me.getValue());
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println("Usage: "
                               +UntangleCompoundComponent.class.getName()
                               +" DB VERSION [COMPONENTS...]");
            System.exit(1);
        }
        
        Integer version = 0;
        try {
            version = Integer.parseInt(argv[1]);
            if (version == 0) {
                System.err.println("VERSION can't be 0");
                System.exit(1);
            }
        }
        catch (NumberFormatException ex) {
            System.err.println("VERSION must be numerical, e.g., 1, 2,...");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            //dumpComponents (ef);

            DataSource dsource =
                ef.getDataSourceFactory().register("stitch_v"+version);
            
            if (argv.length == 2) {
                // do all components
                logger.info("Untangle all components...");
                List<Long> components = new ArrayList<>();
                ef.components(component -> {
                        /*
                        logger.info("Component "+component.getId()+"...");
                        ef.untangle(new UntangleCompoundComponent
                                    (dsource, component));
                        */
                        components.add(component.root().getId());
                    });
                logger.info("### "+components.size()+" components!");
                for (Long cid : components) {
                    logger.info("########### Untangle component "+cid+"...");
                    ef.untangle(new UntangleCompoundComponent
                                (dsource, ef.component(cid)));
                }
            }
            else {
                for (int i = 2; i < argv.length; ++i) {
                    Component comp = ef.component(Long.parseLong(argv[i]));
                    /*
                    logger.info("Dumping component "+comp.getId());         
                    FileOutputStream fos = new FileOutputStream
                        ("Component_"+comp.getId()+".txt");
                    Util.dump(fos, comp);
                    fos.close();
                    */

                    logger.info("Stitching component "+comp.getId());
                    ef.untangle(new UntangleCompoundComponent (dsource, comp));
                }
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
