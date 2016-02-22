package ix.curation;

import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

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

public class CNode implements Props, Comparable<CNode> {
    static final Logger logger = Logger.getLogger(CNode.class.getName());

    public static final String NODE_INDEX = ".node_index";
    public static final String RELATIONSHIP_INDEX = ".relationship_index";
    
    protected final Node _node;
    protected GraphDatabaseService gdb;
    protected TimelineIndex timeline;
    protected DataSourceFactory dsf;
    
    protected Long created;
    protected Long lastUpdated;

    protected CNode (Node node) {
        if (node == null)
            throw new IllegalArgumentException
                ("Entity can't have null backing node!");
        
        gdb = node.getGraphDatabase();
        dsf = new DataSourceFactory (GraphDb.getInstance(gdb));
        
        Index<Node> index = gdb.index().forNodes("node.timeline");
        timeline = new LuceneTimeline (gdb, index);
        
        if (node.hasProperty(CREATED)) {
            created = (Long)node.getProperty(CREATED);
            lastUpdated = (Long)node.getProperty(UPDATED);
        }
        else {
            // new node..
            created = lastUpdated = System.currentTimeMillis();
            node.setProperty(CREATED, created);
            node.setProperty(UPDATED, lastUpdated);
            node.setProperty(KIND, getClass().getName());
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
        snapshot.setProperty(CREATED, lastUpdated);
        if (oldVal != null)
            snapshot.setProperty(OLDVAL, oldVal);
        if (newVal != null) {
            snapshot.setProperty(NEWVAL, newVal);
            _node.setProperty(key, newVal);
        }
        else {
            _node.removeProperty(key);
        }
        Relationship rel = snapshot.createRelationshipTo
            (_node, DynamicRelationshipType.withName(key+".SNAPSHOT"));
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
        for (String index : gdb.index().nodeIndexNames()) 
            gdb.index().forNodes(index).remove(node);
        
        node.delete();
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
}
