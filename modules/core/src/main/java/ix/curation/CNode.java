package ix.curation;

import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.TreeSet;
import java.util.Comparator;
import java.util.Collections;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.index.lucene.LuceneTimeline;
import org.neo4j.index.lucene.TimelineIndex;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class CNode implements Props, Comparable<CNode> {
    static final Logger logger = Logger.getLogger(CNode.class.getName());

    public static final String NODE_INDEX = ".node_index";
    public static final String RELATIONSHIP_INDEX = ".relationship_index";
    public static final String PARENT_INDEX = ".parent_index";
    public static final String NODE_TIMELINE = "node.timeline";

    protected ObjectMapper mapper = new ObjectMapper ();
    protected final Node _node;
    protected GraphDatabaseService gdb;
    protected TimelineIndex<Node> timeline;
    protected DataSourceFactory dsf;
    
    protected Long created;
    protected Long lastUpdated;

    protected CNode (Node node) {
        if (node == null)
            throw new IllegalArgumentException
                ("Entity can't have null backing node!");
        
        gdb = node.getGraphDatabase();
        dsf = new DataSourceFactory (GraphDb.getInstance(gdb));
        
        Index<Node> index = gdb.index().forNodes(NODE_TIMELINE);
        timeline = new LuceneTimeline (gdb, index);
        
        if (node.hasProperty(CREATED)) {
            created = (Long)node.getProperty(CREATED);
            lastUpdated = node.hasProperty(UPDATED)
                ? (Long)node.getProperty(UPDATED) : created;
        }
        else {
            // new node..
            created = lastUpdated = System.currentTimeMillis();
            node.setProperty(CREATED, created);
            node.setProperty(UPDATED, lastUpdated);
            node.setProperty(KIND, getClass().getName());
            node.setProperty(PARENT, node.getId()); // self
            timeline.add(node, created);
        }
        _node = node;
    }
    
    public Node _node () { return _node; }
    public long getId () { return _node.getId(); }
    public int compareTo (CNode node) {
        long d = created - node.created;
        if (d == 0l) {
            d = lastUpdated - node.lastUpdated;
        }
        
        if (d == 0l)
            return 0;
        
        return d < 0l ? -1 : 1;
    }

    public Node _parent () {
        Node n = _node;
        do {
            Long id = (Long)n.getProperty(PARENT, null);
            if (id == null)
                throw new RuntimeException
                    ("Node "+n.getId()+" doesn't a parent!");
            
            if (id == n.getId())
                break;
            n = gdb.getNodeById(id);
            
        } while (true);
        return n;
    }

    public Long lastUpdated () { return lastUpdated; }
    public Long created () { return created; }

    public void _addLabel (String... labels) {
        for (String l : labels) {
            _node.addLabel(DynamicLabel.label(l));
        }
    }

    public void addLabel (String... labels) {
        try (Transaction tx = gdb.beginTx()) {
            _addLabel (labels);
            tx.success();
        }
    }
    
    public void _addLabel (Label label) {
        _node.addLabel(label);
    }

    public Set<String> _labels () {
        Set<String> labels = new TreeSet<String>();
        for (Label l : _node.getLabels()) {
            labels.add(l.name());
        }
        return labels;
    }

    public Set<String> labels () {
        try (Transaction tx = gdb.beginTx()) {
            return _labels ();
        }
    }

    public boolean _is (Label label) {
        return _node.hasLabel(label);
    }

    public boolean _is (String label) {
        return _node.hasLabel(DynamicLabel.label(label));
    }

    public boolean is (String label) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            return _is (label);
        }
    }

    public boolean is (Label label) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            return _is (label);
        }
    }
    
    public DataSource datasource () {
        try (Transaction tx = gdb.beginTx()) {
            if (_node.hasProperty(SOURCE)) {
                return dsf.getDataSourceByKey
                    ((String)_node.getProperty(SOURCE));
            }
        }
        return null;
    }
    
    public void _snapshot (String key, Object value) {
        Object old = null;
        if (_node.hasProperty(key)) {
            old = _node.getProperty(key);
        }
        _snapshot (key, old, value);
    }
    
    public void snapshot (String key, Object value) {
        try (Transaction tx = gdb.beginTx()) {
            _snapshot (key, value);
        }
    }
    
    public void _snapshot (String key, Object oldVal, Object newVal) {
        // update the timeline
        lastUpdated = System.currentTimeMillis();
        _node.setProperty(UPDATED, lastUpdated);
        Node snapshot = gdb.createNode(AuxNodeType.SNAPSHOT);
        snapshot.setProperty(PARENT, _node.getId());
        snapshot.setProperty(KEY, key);
        snapshot.setProperty(CREATED, lastUpdated);
        snapshot.setProperty(UPDATED, lastUpdated);
        if (oldVal != null)
            snapshot.setProperty(OLDVAL, oldVal);
        if (newVal != null) {
            snapshot.setProperty(NEWVAL, newVal);
            _node.setProperty(key, newVal);
        }
        else {
            _node.removeProperty(key);
        }
        /*
        Relationship rel = snapshot.createRelationshipTo
        (_node, DynamicRelationshipType.withName(key+".SNAPSHOT"));*/
        timeline.add(_node, lastUpdated);
    }

    public GraphDatabaseService getGraphDb () { return gdb; }

    public void _delete () {
        _delete (_node);
    }

    public void delete () {
        try (Transaction tx = gdb.beginTx()) {
            _delete ();
            tx.success();
        }
    }

    public static void _delete (Node node) {
        GraphDatabaseService gdb = node.getGraphDatabase();

        for (Relationship rel : node.getRelationships()) {
            Node xn = rel.getOtherNode(node);
            if (xn.hasLabel(AuxNodeType.SNAPSHOT)
                || rel.isType(AuxRelType.PAYLOAD)) {
                rel.delete();
                xn.delete();
            }
        }

        final List<Node> children = new ArrayList<Node>();
        AtomicLong newRoot = new AtomicLong ();
        AtomicInteger rank = new AtomicInteger ();
        gdb.findNodes(AuxNodeType.ENTITY, PARENT, node.getId())
            .stream().forEach(n -> {
                    if (!n.equals(node)) {
                        Integer r = (Integer)n.getProperty(RANK);
                        if (newRoot.get() == 0l || r > rank.get()) {
                            newRoot.set(n.getId());
                            rank.set(r);
                        }
                        children.add(n);
                    }
                });

        if (newRoot.get() != 0l) {
            for (Node n : children)
                n.setProperty(PARENT, newRoot.get());
        }

        for (String index : gdb.index().nodeIndexNames()) 
            gdb.index().forNodes(index).remove(node);
        
        node.delete();
    }

    protected static Node getRoot (Node node) {
        GraphDatabaseService g = node.getGraphDatabase();
        Long id = (Long)node.getProperty(PARENT);
        while (id != node.getId()) {
            node = g.getNodeById(id);
            id = (Long)node.getProperty(PARENT);
        }
        return node;
    }

    protected static boolean find (Node p, Node q) {
        return getRoot(p).equals(getRoot (q));
    }

    protected boolean connected (Node n) {
        return find (_node, n);
    }

    /*
     * does node belong to the same connected component?
     */
    public boolean connected (long id) {
        try (Transaction tx = gdb.beginTx()) {
            return connected (gdb.getNodeById(id));
        }
    }

    public boolean connected (CNode n) {
        return connected (n._node);
    }

    protected static void union (Node p, Node q) {
        Node P = getRoot (p);
        Node Q = getRoot (q);
        if (!P.equals(Q)) {
            int rankp = (Integer)P.getProperty(RANK);
            int rankq = (Integer)Q.getProperty(RANK);
            long dif = rankp - rankq;
            if (dif == 0) {
                // if two ranks are the same, we designate the older
                // node as the parent
                dif = Q.getId() - P.getId();
            }
            if (dif < 0) { // P -> Q
                P.setProperty(PARENT, Q.getId());
                P.removeLabel(AuxNodeType.COMPONENT);
                Q.setProperty(RANK, rankq+rankp);
            }
            else { // Q -> P
                Q.setProperty(PARENT, P.getId());
                Q.removeLabel(AuxNodeType.COMPONENT);
                P.setProperty(RANK, rankq+rankp);
            }
        }
    }
    
    public void execute (Runnable r) {
        try (Transaction tx = gdb.beginTx()) {
            r.run();
            tx.success();
        }
    }

    public <V> V execute (Callable<V> c) {
        V result = null;
        try (Transaction tx = gdb.beginTx()) {
            result = c.call();
            tx.success();
        }
        catch (Exception ex) {
            logger.log(Level.SEVERE, "Can't execute callable", ex);
        }
        return result;  
    }

    public static void _delete (Node node, String name, Object value) {
        if (!node.hasProperty(name))
            return;

        Object old = node.getProperty(name);
        Object delta = Util.delta(old, value); // remove value from old
        if (delta != Util.NO_CHANGE) {
            if (delta != null) {
                node.setProperty(name, delta);
                if (value.getClass().isArray()) {
                    Index<Node> index = _nodeIndex (node);
                    int len = Array.getLength(value);
                    for (int i = 0; i < len; ++i) {
                        index.remove(node, name, Array.get(value, i));
                    }
                }
                else {
                    _nodeIndex(node).remove(node, name, value);
                }
            }
            else {
                node.removeProperty(name);
                _nodeIndex(node).remove(node, name);
            }
            
            // record the change
            new CNode (node)._snapshot(name, old, delta);
        }
    }

    protected Index<Node> _nodeIndex () {
        return gdb.index().forNodes(getClass().getName()+NODE_INDEX);
    }

    protected Index<Node> _parentIndex () {
        return gdb.index().forNodes(getClass().getName()+PARENT_INDEX);
    }
    
    protected RelationshipIndex _relationshipIndex () {
        return gdb.index().forRelationships
            (getClass().getName()+RELATIONSHIP_INDEX);
    }

    public static Index<Node> _nodeIndex (Node node) {
        if (!node.hasProperty(KIND)) {
            throw new IllegalArgumentException
                ("Node "+node.getId()+" doesn't have property "+KIND);
        }
        String kind = (String)node.getProperty(KIND);
        return node.getGraphDatabase().index().forNodes(kind+NODE_INDEX);
    }

    public static RelationshipIndex _relationshipIndex (Node node) {
        if (!node.hasProperty(KIND)) {
            throw new IllegalArgumentException
                ("Node "+node.getId()+" doesn't have property "+KIND);
        }
        String kind = (String)node.getProperty(KIND);
        return node.getGraphDatabase()
            .index().forRelationships(kind+RELATIONSHIP_INDEX);
    }

    @Override
    public int hashCode () { return _node.hashCode(); }
    
    @Override
    public boolean equals (Object obj) {
        return obj instanceof CNode && _node.equals(((CNode)obj)._node);
    }

    public CNode _getLastUpdatedNode () {
        Node node = timeline.getLast();
        return node != null ?  new CNode (node) : null;
    }
    
    public CNode getLastUpdatedNode () {
        try (Transaction tx = gdb.beginTx()) {
            return _getLastUpdatedNode ();
        }
    }

    public JsonNode toJson () {
        try (Transaction tx = gdb.beginTx()) {
            return _toJson ();
        }
    }
    
    public JsonNode _toJson () {
        ObjectNode node = mapper.createObjectNode();
        node.put("id", _node.getId());

        if (_node.hasProperty(KIND))
            node.put(KIND, (String)_node.getProperty(KIND));
        
        if (_node.hasProperty(NAME))
            node.put(NAME, (String)_node.getProperty(NAME));
        
        if (_node.hasProperty(KEY))
            node.put(KEY, (String)_node.getProperty(KEY));
            
        node.put(CREATED, (Long)_node.getProperty(CREATED));
        if (_node.hasProperty(UPDATED))
            node.put(UPDATED, (Long)_node.getProperty(UPDATED));

        DataSource ds = datasource ();
        if (ds != null) {
            ObjectNode src = mapper.createObjectNode();
            src.put("id", ds.getId());
            src.put("key", ds.getKey());
            src.put("name", ds.getName());
            node.put("datasource", src);
        }

        ArrayNode array = mapper.createArrayNode();
        for (String l : _labels ()) {
            if (ds != null && l.equals(ds._getKey()))
                ;
            else
                array.add(l);
        }
        node.put("labels", array);

        ArrayNode neighbors = mapper.createArrayNode();
        List<Relationship> snapshots = new ArrayList<Relationship>();
        Node parent = null;
        Node payload = null;
        for (Relationship rel : _node.getRelationships(Direction.BOTH)) {
            Node n = rel.getOtherNode(_node);
            if (n.hasLabel(AuxNodeType.SNAPSHOT)) {
                snapshots.add(rel);
            }
            else if (_node.hasLabel(AuxNodeType.SNAPSHOT)
                     || _node.hasLabel(AuxNodeType.DATA)) {
                // there should only be one edge for snapshot node!
                parent = n;
            }
            else if (n.hasLabel(AuxNodeType.DATA)) {
                payload = n;
            }
            else if (n.hasLabel(AuxNodeType.COMPONENT)) {
                // should do something here..
            }
            else {
                ObjectNode nb = mapper.createObjectNode();
                nb.put("id", n.getId());
                if (null != rel.getType())
                    nb.put("reltype", rel.getType().name());
                for (Map.Entry<String, Object> me :
                         rel.getAllProperties().entrySet()) {
                    nb.put(me.getKey(), mapper.valueToTree(me.getValue()));
                }
                neighbors.add(nb);
            }
        }
            
        if (neighbors.size() > 0)
            node.put("neighbors", neighbors);
            
        if (payload != null) {
            ObjectNode pl = mapper.createObjectNode();
            for (Map.Entry<String, Object> me :
                     payload.getAllProperties().entrySet()) {
                Object value = me.getValue();
                pl.put(me.getKey(), mapper.valueToTree(value));
            }
            ObjectNode load = mapper.createObjectNode();
            load.put("id", payload.getId());
            load.put("content", pl);
            node.put("payload", load);
        }
            
        if (!snapshots.isEmpty()) {
            Collections.sort(snapshots, new Comparator<Relationship>() {
                    public int compare (Relationship r1,
                                        Relationship r2) {
                        Long t2 = (Long)r2.getOtherNode(_node)
                            .getProperty(CREATED);
                        Long t1 = (Long)r1.getOtherNode(_node)
                            .getProperty(CREATED);
                        if (t1 == t2) return 0;
                        return t1 < t2 ? -1 : 1;
                    }
                });

            array = mapper.createArrayNode();
            for (Relationship rel : snapshots) {
                Node n = rel.getOtherNode(_node);
                String key = rel.getType().name();
                int pos = key.indexOf(".SNAPSHOT");
                if (pos > 0) {
                    key = key.substring(0, pos);
                }
                ObjectNode sn = mapper.createObjectNode();
                sn.put("property", key);
                sn.put("timestamp", (Long)n.getProperty(CREATED));
                if (n.hasProperty(OLDVAL)) {
                    sn.put("oldval",
                           mapper.valueToTree(n.getProperty(OLDVAL)));
                }
                if (n.hasProperty(NEWVAL)) {
                    sn.put("newval",
                           mapper.valueToTree(n.getProperty(NEWVAL)));
                }
                array.add(sn);
            }
            node.put("snapshots", array);
        }
        else {
            if (parent != null) {
                node.put("parent", parent.getId());
                array = mapper.createArrayNode();
                for (Map.Entry<String, Object> me :
                         _node.getAllProperties().entrySet()) {
                    ObjectNode n = mapper.createObjectNode();
                    n.put("key", me.getKey());
                    n.put("value", mapper.valueToTree(me.getValue()));
                    array.add(n);
                }
                node.put("properties", array);
            }
                    
            if (_node.hasProperty(OLDVAL))
                node.put("oldval",
                         mapper.valueToTree(_node.getProperty(OLDVAL)));

            if (_node.hasProperty(NEWVAL))
                node.put("newval",
                         mapper.valueToTree(_node.getProperty(NEWVAL)));
        }
        return node;
    }
}
