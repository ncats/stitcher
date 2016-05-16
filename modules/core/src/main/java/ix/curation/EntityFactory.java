package ix.curation;

import java.util.*;
import java.io.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.lang.reflect.Array;

import java.util.concurrent.Callable;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.index.lucene.LuceneTimeline;
import org.neo4j.index.lucene.TimelineIndex;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import ix.curation.graph.UnionFind;

// NOTE: methods and variables that begin with underscore "_" generally assume that a graph database transaction is already open!

public class EntityFactory implements Props {
    static final Logger logger = Logger.getLogger
        (EntityFactory.class.getName());

    static class DefaultGraphMetrics implements GraphMetrics {
        int entityCount;
        Map<EntityType, Integer> entityHistogram =
            new EnumMap<EntityType, Integer>(EntityType.class);
        Map<Integer, Integer> entitySizeDistribution =
            new TreeMap<Integer, Integer>();
        int stitchCount;
        Map<String, Integer> stitchHistogram =
            new TreeMap<String, Integer>();
        int connectedComponentCount;
        Map<Integer, Integer> connectedComponentHistogram =
            new TreeMap<Integer, Integer>();
        int singletonCount;

        DefaultGraphMetrics () {}

        public int getEntityCount () { return entityCount; }
        public Map<EntityType, Integer> getEntityHistogram () {
            return entityHistogram;
        }
        public Map<Integer, Integer> getEntitySizeDistribution () {
            return entitySizeDistribution;
        }
        public int getStitchCount () { return stitchCount; }
        public Map<String, Integer> getStitchHistogram () {
            return stitchHistogram;
        }
        public int getConnectedComponentCount () {
            return connectedComponentCount;
        }
        public Map<Integer, Integer> getConnectedComponentHistogram () {
            return connectedComponentHistogram;
        }
        public int getSingletonCount () { return singletonCount; }
    }

    class EntityIterator implements Iterator<Entity> {
        final Iterator<Node> iter;

        EntityIterator (Iterator<Node> iter) {
            this.iter = iter;
        }

        public boolean hasNext () {
            try (Transaction tx = gdb.beginTx()) {
                return iter.hasNext();
            }
        }
        
        public Entity next () {
            Node n = iter.next();
            return Entity.getEntity(n);
        }
        
        public void remove () {
            throw new UnsupportedOperationException ("remove not supported");
        }
    }

    static class ConnectedComponents implements Iterator<Entity[]> {
        int current;
        long[][] components;
        long[] singletons;
        final GraphDatabaseService gdb;
        
        ConnectedComponents (GraphDatabaseService gdb) {
            try (Transaction tx = gdb.beginTx()) {
                UnionFind eqv = new UnionFind ();
                List<Long> singletons = new ArrayList<Long>();

                gdb.findNodes(AuxNodeType.ENTITY).stream().forEach(node -> {
                        int edges = 0;
                        for (Relationship rel
                                 : node.getRelationships(Direction.BOTH,
                                                         Entity.KEYS)) {
                            eqv.union(rel.getStartNode().getId(),
                                      rel.getEndNode().getId());
                            ++edges;
                        }
                        
                        if (edges == 0) {
                            singletons.add(node.getId());
                        }
                    });
                
                this.singletons = new long[singletons.size()];
                for (int i = 0; i < this.singletons.length; ++i)
                    this.singletons[i] = singletons.get(i);
                components = eqv.components();
                tx.success();
            }
            this.gdb = gdb;
        }

        public long[][] components () {
            return components;
        }
        
        public long[] singletons () {
            return singletons;
        }

        public boolean hasNext () {
            boolean next = current < components.length;
            if (!next) {
                next = (current - components.length) < singletons.length;
            }
            return next;
        }
        
        public Entity[] next () {
            Entity[] comp;
            if (current < components.length) {
                long[] cc = components[current];
                
                comp = new Entity[cc.length];
                try (Transaction tx = gdb.beginTx()) {
                    for (int i = 0; i < cc.length; ++i) {
                        comp[i] = new Entity (gdb.getNodeById(cc[i]));
                    }
                    tx.success();
                }
            }
            else {
                comp = new Entity[1];
                try (Transaction tx = gdb.beginTx()) {
                    comp[0] = new Entity
                        (gdb.getNodeById
                         (singletons[current-components.length]));
                    tx.success();
                }
            }
            ++current;
            
            return comp;
        }
        
        public void remove () {
            throw new UnsupportedOperationException ("remove not supported");
        }
    }

    /*
     * a simple graph wrapper around a set of nodes
     */
    static class Graph {
        final BitSet[] adj;
        final StitchKey key;
        
        Graph (GraphDatabaseService gdb, StitchKey key, long[] nodes) {
            adj = new BitSet[nodes.length];
            try (Transaction tx = gdb.beginTx()) {
                for (int i = 0; i < nodes.length; ++i) {
                    Node n = gdb.getNodeById(nodes[i]);
                    BitSet bs = new BitSet (nodes.length);
                    for (Relationship rel :
                             n.getRelationships(key, Direction.BOTH)) {
                        Node m = rel.getOtherNode(n);
                        long id = m.getId();
                        for (int j = 0; j < nodes.length; ++j) 
                            if (i != j && nodes[j] == id) {
                                bs.set(j);
                                break;
                            }
                    }
                    adj[i] = bs;
                }
                tx.success();
            }
            this.key = key;
        }
        
        public BitSet edges (int n) { return adj[n]; }
        public StitchKey key () { return key; }
    }

    static class ComponentImpl implements Component {
        Set<Long> nodes = new TreeSet<Long>();
        Entity[] entities;
        String id;
        Node root;

        ComponentImpl (Node node) {
            GraphDatabaseService gdb = node.getGraphDatabase();
            try (Transaction tx = gdb.beginTx()) {
                if (!node.hasLabel(AuxNodeType.COMPONENT)
                    || !node.hasProperty(CNode.RANK))
                    throw new IllegalArgumentException
                        ("Not a valid component node: "+node.getId());
                traverse (gdb, node);

                Integer rank = (Integer)node.getProperty(CNode.RANK);
                if (rank != nodes.size())
                    logger.warning("Rank is "+rank+" but there are "
                                   +nodes.size()+" nodes in this component!");
                
                entities = new Entity[nodes.size()];
                int i = 0;
                for (Long id : nodes)
                    entities[i++] = Entity._getEntity(gdb.getNodeById(id));
                
                tx.success();
            }
            id = Util.sha1(nodes).substring(0, 9);
            root = node;
        }

        void traverse (GraphDatabaseService gdb, Node node) {
            gdb.findNodes(CNode.CLASS_LABEL,
                          Props.PARENT, node.getId()).stream()
                .forEach(n -> {
                        Long pid = (Long)n.getProperty(Props.PARENT);
                        nodes.add(n.getId());
                        if (!pid.equals(n.getId()))
                            traverse (gdb, n);
                    });
        }

        public String getId () { return id; }
        public Entity[] entities () { return entities; }
        public int size () { return nodes.size(); }
        public Set<Long> nodes () { return nodes; }
        public int hashCode () { return nodes.hashCode(); }
        public boolean equals (Object obj) {
            if (obj instanceof ComponentImpl) {
                return nodes.equals(((ComponentImpl)obj).nodes);
            }
            return false;
        }
    }

    static class CliqueImpl implements Clique {
        final EnumMap<StitchKey, Object> values =
            new EnumMap (StitchKey.class);
        Entity[] entities;
        Set<Long> clique = new TreeSet<Long>();
        String id;
        
        CliqueImpl (BitSet C, long[] nodes, Set<StitchKey> keys,
                 GraphDatabaseService gdb) {
            entities = new Entity[C.cardinality()];
            try (Transaction tx = gdb.beginTx()) {
                Node[] gnodes = new Node[C.cardinality()];
                for (int i = C.nextSetBit(0), j = 0;
                     i >= 0; i = C.nextSetBit(i+1)) {
                    gnodes[j] = gdb.getNodeById(nodes[i]);
                    entities[j] = Entity._getEntity(gnodes[j]);
                    clique.add(nodes[i]);
                    ++j;
                }

                for (StitchKey key : keys)
                    update (gnodes, key);
                tx.success();
            }
            id = Util.sha1(clique).substring(0, 9);
        }

        void update (Node[] nodes, StitchKey key) {
            final Map<Object, Integer> counts = new HashMap<Object, Integer>();
            for (int i = 0; i < nodes.length; ++i) {
                for (Relationship rel
                         : nodes[i].getRelationships(key, Direction.BOTH)) {
                    for (int j = i+1; j < nodes.length; ++j) {
                        if (nodes[j].equals(rel.getOtherNode(nodes[i]))) {
                            if (rel.hasProperty(VALUE)) {
                                Object val = rel.getProperty(VALUE);
                                Integer c = counts.get(val);
                                counts.put(val, c != null ? c+1:1);
                            }
                        }
                    }
                }
            }

            int total = nodes.length*(nodes.length-1)/2;
            Object value = null;            
            for (Map.Entry<Object, Integer> me : counts.entrySet()) {
                Integer c = me.getValue();
                if (c == total) {
                    value = me.getKey();
                    break;
                }
                else /*if (c > 1)*/ { // multiple values for this stitchkey
                    value = value == null
                        ? me.getKey() : Util.merge(value, me.getKey());
                }
            }

            if (value != null && value.getClass().isArray()) {
                // make sure it's sorted from most frequent to least
                Object[] sorted = new Object[Array.getLength(value)];
                for (int i = 0; i < sorted.length; ++i)
                    sorted[i] = Array.get(value, i);
                
                Arrays.sort(sorted, new Comparator () {
                        public int compare (Object o1, Object o2) {
                            Integer c1 = counts.get(o1);
                            Integer c2 = counts.get(o2);
                            return c2 - c1;
                        }
                    });
                
                for (int i = 0; i < sorted.length; ++i)
                    Array.set(value, i, sorted[i]);
            }

            values.put(key, value);
        }

        public String getId () { return id; }
        public int hashCode () { return clique.hashCode(); }
        public boolean equals (Object obj) {
            if (obj instanceof CliqueImpl) {
                return clique.equals(((CliqueImpl)obj).clique);
            }
            return false;
        }
        public int size () { return clique.size(); }
        public Set<Long> nodes () { return clique; }
        public Map<StitchKey, Object> values () { return values; }
        public Entity[] entities () { return entities; }
        public Entity[] entities (Clique C) {
            Set<Long> ov = getOverlapNodes (C);
            Entity[] ent = null;
            if (!ov.isEmpty()) {
                ent = new Entity[ov.size()];
                int i = 0;
                for (Entity e : entities)
                    if (ov.contains(e.getId()))
                        ent[i++] = e;
            }
            return ent;
        }
        
        public boolean overlaps (Clique C) {
            return !getOverlapNodes(C).isEmpty();
        }

        Set<Long> getOverlapNodes (Clique C) {
            Set<Long> ref, other;
            if (C.size() < clique.size()) {
                ref = C.nodes();
                other = this.clique;
            }
            else {
                ref = this.clique;
                other = C.nodes();
            }
            
            Set<Long> ov = new HashSet<Long>();
            for (Long id : ref)
                if (other.contains(id))
                    ov.add(id);
            
            return ov;
        }
    }
    
    /*
     * using the standard Bron-Kerbosch enumeration algorithm
     */
    static class CliqueEnumeration {
        final GraphDatabaseService gdb;
        final Map<BitSet, EnumSet<StitchKey>> cliques =
            new HashMap<BitSet, EnumSet<StitchKey>>();
        final StitchKey[] keys;
        
        CliqueEnumeration (GraphDatabaseService gdb, StitchKey[] keys) {
            this.gdb = gdb;
            this.keys = keys;
        }

        public boolean enumerate (long[] nodes, CliqueVisitor visitor) {
            cliques.clear();
            
            for (StitchKey key : keys) enumerate (key, nodes);
            for (Map.Entry<BitSet, EnumSet<StitchKey>> me
                     : cliques.entrySet()) {
                if (!visitor.clique
                    (new CliqueImpl (me.getKey(), nodes, me.getValue(), gdb)))
                    return false;
            }
            return true;
        }
        
        void enumerate (StitchKey key, long[] nodes) {
            BitSet P = new BitSet (nodes.length);
            for (int i = 0; i < nodes.length; ++i) {
                P.set(i);
            }
            BitSet C = new BitSet (nodes.length);
            BitSet S = new BitSet (nodes.length);
            Graph G = new Graph (gdb, key, nodes);
            
            //logger.info("Clique enumeration "+key+" |G|="+nodes.length+"...");
            bronKerbosch (G, C, P, S);
        }

        void bronKerbosch (Graph G, BitSet C, BitSet P, BitSet S) {
            if (P.isEmpty() && S.isEmpty()) {
                // only consider cliques that are of size >= 3
                if (C.cardinality() >= 3) {
                    BitSet c = (BitSet)C.clone();
                    
                    EnumSet<StitchKey> keys = cliques.get(c);
                    if (keys == null) {
                        cliques.put(c, EnumSet.of(G.key()));
                    }
                    else
                        keys.add(G.key());
                }
            }
            else {
                for (int u = P.nextSetBit(0); u >=0 ; u = P.nextSetBit(u+1)) {
                    P.clear(u);
                    BitSet PP = (BitSet)P.clone();
                    BitSet SS = (BitSet)S.clone();
                    PP.and(G.edges(u));
                    SS.and(G.edges(u));
                    C.set(u);
                    bronKerbosch (G, C, PP, SS);
                    C.clear(u);
                    S.set(u);
                }
            }
        }
    }

    protected final GraphDb graphDb;
    protected final GraphDatabaseService gdb;
    protected final TimelineIndex<Node> timeline;
    
    public EntityFactory (String dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }
    
    public EntityFactory (File dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }

    public EntityFactory (GraphDatabaseService gdb) {
        this (GraphDb.getInstance(gdb));
    }
    
    public EntityFactory (GraphDb graphDb) {
        if (graphDb == null)
            throw new IllegalArgumentException ("GraphDb instance is null!");
        
        this.graphDb = graphDb;
        this.gdb = graphDb.graphDb();
        try (Transaction tx = gdb.beginTx()) {
            this.timeline = new LuceneTimeline
                (gdb, gdb.index().forNodes(CNode.NODE_TIMELINE));
            tx.success();
        }
    }

    public GraphDb getGraphDb () { return graphDb; }
    public CacheFactory getCache () { return graphDb.getCache(); }
    public void setCache (CacheFactory cache) {
        graphDb.setCache(cache);
    }
    public void setCache (String cache) throws IOException {
        graphDb.setCache(CacheFactory.getInstance(cache));
    }
    
    public long getLastUpdated () { return graphDb.getLastUpdated(); }
    
    public GraphMetrics calcGraphMetrics() {
        return calcGraphMetrics (gdb, AuxNodeType.ENTITY,
                                    Entity.TYPES, Entity.KEYS);
    }

    public GraphMetrics calcGraphMetrics (String label) {
        return calcGraphMetrics (DynamicLabel.label(label));
    }
    
    public GraphMetrics calcGraphMetrics (Label label) {
        return calcGraphMetrics (gdb, label, Entity.TYPES, Entity.KEYS);
    }

    public static GraphMetrics calcGraphMetrics
        (GraphDatabaseService gdb, EntityType[] types,
         RelationshipType[] keys) {
        return calcGraphMetrics (gdb, AuxNodeType.ENTITY, types, keys);
    }
    
    public static GraphMetrics calcGraphMetrics
        (GraphDatabaseService gdb, Label label,
         EntityType[] types, RelationshipType[] keys) {
        
        DefaultGraphMetrics metrics = new DefaultGraphMetrics ();
        UnionFind eqv = new UnionFind ();
        
        try (Transaction tx = gdb.beginTx()) {
            ResourceIterator<Node> nodes = gdb.findNodes(label);
            nodes.stream().forEach(node -> {
                    for (EntityType t : types) {
                        if (node.hasLabel(t)) {
                            Integer c = metrics.entityHistogram.get(t);
                            metrics.entityHistogram.put(t, c != null ? c+1:1);
                        }
                    }
                    int nrel = 0;
                    for (Relationship rel
                             : node.getRelationships(Direction.BOTH, keys)) {
                        Node xn = rel.getOtherNode(node);
                        // do we count self reference?
                        if (xn.hasLabel(label)) {
                            if (!xn.equals(node)) {
                                eqv.union(node.getId(), xn.getId());
                                ++metrics.stitchCount;
                            }
                            String key = rel.getType().name();
                            Integer c = metrics.stitchHistogram.get(key);
                            metrics.stitchHistogram.put(key, c != null ? c+1:1);
                            ++nrel;
                        }
                    }
                    Integer c = metrics.entitySizeDistribution.get(nrel);
                    metrics.entitySizeDistribution.put(nrel, c!=null ? c+1:1);
                    
                    ++metrics.entityCount;
                });
            nodes.close();
            
            // we're double counting, so now we correct the counts
            metrics.stitchCount /= 2;
            for (String k : metrics.stitchHistogram.keySet()) {
                metrics.stitchHistogram.put
                    (k, metrics.stitchHistogram.get(k)/2);
            }

            // now assign each node to its respective connected component
            long[][] comps = eqv.components();
            Map<Long, Integer> labels = new HashMap<Long, Integer>();
            for (int c = 0; c < comps.length; ++c) {
                for (int i = 0; i < comps[c].length; ++i) {
                    labels.put(comps[c][i], c+1);
                }
                Integer cnt = metrics.connectedComponentHistogram.get
                    (comps[c].length);
                metrics.connectedComponentHistogram.put
                    (comps[c].length, cnt!= null ? cnt+1:1);
            }
            AtomicInteger cc = new AtomicInteger (comps.length);
            
            // now tag each node
            if (!label.name().startsWith("CC_")) {
                nodes = gdb.findNodes(label);
                nodes.stream().forEach(node -> {
                        for (Label l: node.getLabels())
                            if (l.name().startsWith("CC_")
                                || l.name().equals(AuxNodeType.SINGLETON.name()))
                                node.removeLabel(l);
                        Integer c = labels.get(node.getId());
                        if (c == null) {
                            node.addLabel(AuxNodeType.SINGLETON);
                            ++metrics.singletonCount;
                            c = cc.incrementAndGet(); // increment the cc id
                        }
                        node.addLabel(DynamicLabel.label("CC_"+c));
                    });
                nodes.close();
            }
            
            if (metrics.singletonCount > 0) {
                metrics.connectedComponentHistogram.put
                    (1, metrics.singletonCount);
            }
            metrics.connectedComponentCount = cc.get();

            tx.success();
        }
        return metrics;
    }

    static boolean hasLabel (Node node, Label... labels) {
        for (Label l : labels)
            if (node.hasLabel(l))
                return true;
        return false;
    }
    
    /*
     * return the top k stitched values for a given stitch key
     */
    public Map<Object, Integer> getStitchedValueDistribution (StitchKey key) {
        return getStitchedValueDistribution (key, (Label[])null);
    }
    
    public Map<Object, Integer> getStitchedValueDistribution
        (StitchKey key, String... labels) {
        Label[] l = null;
        if (labels != null && labels.length > 0) {
            l = new Label[labels.length];
            for (int i = 0; i < labels.length; ++i)
                l[i] = DynamicLabel.label(labels[i]);
        }
        return getStitchedValueDistribution (key, l);
    }
    
    public Map<Object, Integer> getStitchedValueDistribution
        (StitchKey key, Label... labels) {
        Map<Object, Integer> values = new HashMap<Object, Integer>();
        try (Transaction tx = gdb.beginTx()) {
            for (Relationship rel : gdb.getAllRelationships()) {
                if (rel.isType(key) && rel.hasProperty(Entity.VALUE)
                    && (labels == null
                        || (hasLabel (rel.getStartNode(), labels)
                            && hasLabel (rel.getEndNode(), labels)))) {
                        Object val = rel.getProperty(Entity.VALUE);
                        Integer c = values.get(val);
                        values.put(val, c!=null ? c+1:1);
                }
            }
            tx.success();
        }
        return values;
    }

    public int getStitchedValueCount (StitchKey key, Object value) {
        int count = 0;
        try (Transaction tx = gdb.beginTx()) {
            RelationshipIndex index = gdb.index().forRelationships
                (Entity.relationshipIndexName());
            IndexHits<Relationship> hits = index.get(key.name(), value);
            count = hits.size();
            hits.close();
            tx.success();
        }
        return count;
    }

    /*
     * globally delete the value for a particular stitch key
     */
    public void delete (StitchKey key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            RelationshipIndex index =
                gdb.index().forRelationships(Entity.relationshipIndexName());
            IndexHits<Relationship> hits = index.get(key.name(), value);
            try {
                for (Relationship rel : hits) {
                    CNode._delete(rel.getStartNode(), key.name(), value);
                    CNode._delete(rel.getEndNode(), key.name(), value);
                    index.remove(rel);
                    rel.delete();
                }
            }
            finally {
                hits.close();
            }
            tx.success();
        }
    }

    public int delete (DataSource source) {
        return delete (source.getKey());
    }
    
    /*
     * delete the entire data source; note that source can either be
     * the name or its key
     */
    public int delete (String source) {
        int count = 0;
        try (Transaction tx = gdb.beginTx()) {
            Index<Node> index = gdb.index().forNodes
                (DataSource.nodeIndexName());

            Node n = index.get(KEY, source).getSingle();
            if (n == null) {
                source = DataSourceFactory.sourceKey(source);
                n = index.get(KEY, source).getSingle();
            }
            
            if (n != null) {
                Label label = DynamicLabel.label(source);
                for (Iterator<Node> it = gdb.findNodes(label);
                     it.hasNext(); ) {
                    Node node = it.next();
                    Entity._getEntity(node).delete();
                    ++count;
                }
            }
            else {
                logger.warning("Can't find data source: "+source);
            }
            tx.success();
        }
        return count;
    }

    public Iterator<Entity[]> connectedComponents () {
        return new ConnectedComponents (gdb);
    }

    public Collection<Component> components () {
        List<Component> comps = new ArrayList<Component>();
        try (Transaction tx = gdb.beginTx()) {
            gdb.findNodes(AuxNodeType.COMPONENT).stream().forEach(node -> {
                    comps.add(new ComponentImpl (node));
                });
            tx.success();
        }
        
        return comps;
    }

    public Entity[] entities (String label, int skip, int top) {
        return entities (DynamicLabel.label(label), skip, top);
    }
    
    public Entity[] entities (Label label, int skip, int top) {
        List<Entity> page = new ArrayList<Entity>();
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("skip", skip);
        params.put("top", top);

        //System.out.println("components: skip="+skip+" top="+top);
        try (Transaction tx = gdb.beginTx();
             Result result = gdb.execute
             ("match(n:`"+label+"`) return n skip {skip} limit {top}", params)
             ) {
            while (result.hasNext()) {
                Map<String, Object> row = result.next();
                //System.out.println("  rows: "+row);
                Node n = (Node)row.get("n");
                try {
                    page.add(Entity._getEntity(n));
                }
                catch (Exception ex) { // not an entity
                }
            }
            result.close();
            tx.success();
        }
        
        return page.toArray(new Entity[0]);
    }

    public Iterator<Entity> find (StitchKey key, Object value) {
        return find (key.name(), value);
    }

    public boolean cliqueEnumeration (CliqueVisitor visitor) {
        return cliqueEnumeration (Entity.KEYS, visitor);
    }

    public boolean cliqueEnumeration (String label, CliqueVisitor visitor) {
        return label != null ? 
            cliqueEnumeration (Entity.KEYS,
                               DynamicLabel.label(label), visitor)
            : cliqueEnumeration (Entity.KEYS, visitor);
    }
    
    public boolean cliqueEnumeration (StitchKey[] keys,
                                      String label, CliqueVisitor visitor) {
        if (keys == null || keys.length == 0)
            keys = Entity.KEYS;
        
        return label != null ?
            cliqueEnumeration (keys, DynamicLabel.label(label), visitor)
            : cliqueEnumeration (keys, visitor);
    }

    public boolean cliqueEnumeration (StitchKey[] keys,
                                      Label label, CliqueVisitor visitor) {
        try (Transaction tx = gdb.beginTx()) {
            List<Long> ids = new ArrayList<Long>();
            for (Iterator<Node> it = gdb.findNodes(label); it.hasNext(); ) {
                Node n = it.next();
                ids.add(n.getId());
            }
            long[] nodes = new long[ids.size()];
            int i = 0;
            for (Long id : ids) {
                nodes[i++] = id;
            }
            
            return cliqueEnumeration (keys, nodes, visitor);
        }
    }

    public boolean cliqueEnumeration (StitchKey[] keys, CliqueVisitor visitor) {
        ConnectedComponents cc = new ConnectedComponents (gdb);
        long[][] comps = cc.components();
        for (int i = 0; i < comps.length && comps[i].length >= 3; ++i) {
            if (!cliqueEnumeration (keys, comps[i], visitor))
                return false;
        }
        return true;
    }

    public boolean cliqueEnumeration (long[] nodes, CliqueVisitor visitor) {
        return cliqueEnumeration (Entity.KEYS, nodes, visitor);
    }

    public boolean cliqueEnumeration
        (StitchKey[] keys, Entity[] entities, CliqueVisitor visitor) {
        long[] nodes = new long[entities.length];
        try (Transaction tx = gdb.beginTx()) {
            for (int i = 0; i < nodes.length; ++i)
                nodes[i] = entities[i].getId();
            
            return cliqueEnumeration (keys, nodes, visitor);
        }
    }
    
    public boolean cliqueEnumeration(Entity[] entities, CliqueVisitor visitor) {
        return cliqueEnumeration (Entity.KEYS, entities, visitor);
    }
    
    public boolean cliqueEnumeration (StitchKey[] keys,
                                      long[] nodes, CliqueVisitor visitor) {
        { EnumSet<StitchKey> set = EnumSet.noneOf(StitchKey.class);
            for (StitchKey k : keys) set.add(k);
            logger.info("enumerating cliques over "+set+" spanning "
                        +nodes.length+" nodes...");
        }
        
        CliqueEnumeration clique = new CliqueEnumeration (gdb, keys); 
        // enumerate all cliques for this key
        return clique.enumerate(nodes, visitor);
    }
    
    public Iterator<Entity> find (String key, Object value) {
        Iterator<Entity> iterator = null;
        try (Transaction tx = gdb.beginTx()) {
            Index<Node> index = gdb.index().forNodes(Entity.nodeIndexName());
            if (value.getClass().isArray()) {
                int len = Array.getLength(value);
                for (int i = 0; i < len; ++i) {
                    Object v = Array.get(value, i);
                    IndexHits<Node> hits = index.get(key, v);
                    // return on the first non-empty hits
                    if (hits.size() > 0) {
                        iterator = new EntityIterator (hits.iterator());
                        break;
                    }
                }
            }

            if (iterator == null)
                iterator = new EntityIterator
                    (index.get(key, value).iterator());
        }
        return iterator;
    }

    /**
     * iterate over entities of a particular data source
     */
    public Iterator<Entity> entities (DataSource source) {
        return entities (source.getKey());
    }

    public Iterator<Entity> entities (String label) {
        try (Transaction tx = gdb.beginTx()) {
            return new EntityIterator
                (gdb.findNodes(DynamicLabel.label(label)));
        }
    }

    public Entity[] entities (long[] ids) {
        try (Transaction tx = gdb.beginTx()) {
            return _entities (ids);
        }
    }

    public Entity[] _entities (long[] ids) {
        Entity[] entities = new Entity[ids.length];
        for (int i = 0; i < ids.length; ++i)
            entities[i] = _entity (ids[i]);
        return entities;
    }   

    public Entity entity (long id) {
        try (Transaction tx = gdb.beginTx()) {
            return _entity (id);
        }
    }

    public Entity _entity (long id) {
        return Entity._getEntity(gdb.getNodeById(id));  
    }
    
    /**
     * iterate over entities regardless of data source
     */
    public Iterator<Entity> entities () {
        try (Transaction tx = gdb.beginTx()) {
            return new EntityIterator (gdb.findNodes(AuxNodeType.ENTITY));
        }
    }
    
    public void shutdown () {
        graphDb.shutdown();
    }

    public void execute (Runnable r) {
        execute (r, true);
    }
    
    public void execute (Runnable r, boolean commit) {
        try (Transaction tx = gdb.beginTx()) {
            r.run();
            if (commit)
                tx.success();
        }
    }
 
    public <V> V execute (Callable<V> c) {
        return execute (c, true);
    }
    
    public <V> V execute (Callable<V> c, boolean commit) {
        V result = null;
        try (Transaction tx = gdb.beginTx()) {
            result = c.call();
            if (commit)
                tx.success();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't execute callable", ex);
        }
        return result;  
    }

    public Entity createStitch (DataSource source, Entity... children) {
        try (Transaction tx = gdb.beginTx()) {
            Entity ent = _createStitch (source, children);
            tx.success();
            return ent;
        }
    }

    public Entity _createStitch (DataSource source, Entity... children) {
        Node node = gdb.createNode(AuxNodeType.SUPERNODE, AuxNodeType.ENTITY,
                                   DynamicLabel.label(source._getKey()));
        node.setProperty(SOURCE, source._getKey());
        Entity entity = null;
        Set<String> sources = new HashSet<String>();    
        for (Entity e : children) {
            if (entity == null) {
                // inherite the type from the first child
                node.addLabel(e.type());
                entity = Entity._getEntity(node);
            }
            else if (entity.type() != e.type())
                throw new IllegalArgumentException
                    ("Can't stitch entities of different type: "
                     +entity.type()+" and "+e.type());
            entity._stitch(e, null);
            sources.add((String)e._node.getProperty(SOURCE));
        }
        for (String s : sources)
            node.addLabel(DynamicLabel.label(s));
        
        return entity;
    }

    public Entity createStitch (DataSource source, long[] children) {
        try (Transaction tx = gdb.beginTx()) {
            Entity ent = _createStitch (source, children);
            tx.success();
            return ent;
        }
    }

    public Entity _createStitch (DataSource source, long[] children) {
        Entity[] entities = new Entity[children.length];
        for (int i = 0; i < children.length; ++i)
            entities[i] = _entity (children[i]);
        return _createStitch (source, entities);
    }

    // return the last k updated entities
    public Entity[] getLastUpdatedEntities (int k) {
        try (Transaction tx = gdb.beginTx()) {
            return _getLastUpdatedEntities (k);
        }
    }
    
    public Entity[] _getLastUpdatedEntities (int k) {
        IndexHits<Node> hits = timeline.getBetween
            (null, System.currentTimeMillis(), true);
        try {
            int max = Math.min(k, hits.size());
            Entity[] entities = new Entity[max];
            k = 0;
            for (Node n : hits) {
                try {
                    entities[k] = Entity._getEntity(n);
                    if (++k == entities.length)
                        break;
                }
                catch (IllegalArgumentException ex) {
                    // not an Entity node
                }
            }
            return entities;
        }
        finally {
            hits.close();
        }
    }

    public Entity getLastUpdatedEntity () {
        Entity[] ent = getLastUpdatedEntities (1);
        return ent != null && ent.length > 0 ? ent[0] : null;
    }
}
