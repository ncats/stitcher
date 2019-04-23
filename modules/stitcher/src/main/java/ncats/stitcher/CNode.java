package ncats.stitcher;

import java.util.*;
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

import chemaxon.struc.Molecule;
import chemaxon.util.MolHandler;

public class CNode implements Props, Comparable<CNode> {
    static final Logger logger = Logger.getLogger(CNode.class.getName());

    public static final String NODE_INDEX = ".node_index";
    public static final String RELATIONSHIP_INDEX = ".relationship_index";
    public static final String PARENT_INDEX = ".parent_index";
    public static final String NODE_TIMELINE = "node.timeline";

    static final String[] MOLFIELDS = new String[]{
        "MOLFILE",
        "SMILES",
        "SMILES_ISO"
    };

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
            if (!node.hasProperty(PARENT)) {
                node.setProperty(PARENT, node.getId()); // self
            }
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
            _node.addLabel(Label.label(l));
        }
    }

    public void addLabel (String... labels) {
        try (Transaction tx = gdb.beginTx()) {
            _addLabel (labels);
            tx.success();
        }
    }
    
    public void _addLabel (Label... labels) {
        for (Label l : labels)
            _node.addLabel(l);
    }

    public void addLabel (Label... labels) {
        try (Transaction tx = gdb.beginTx()) {
            _addLabel (labels);
            tx.success();
        }
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
            Set<String> labels = _labels ();
            tx.success();
            return labels;
        }
    }

    public boolean _hasAnyLabels (Label... labels) {
        for (Label l : labels)
            if (_node.hasLabel(l))
                return true;
        return false;
    }

    public boolean _hasAllLabels (Label... labels) {
        for (Label l : labels)
            if (!_node.hasLabel(l))
                return false;
        return true;
    }

    public boolean hasAllLabels (Label... labels) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            boolean all = _hasAllLabels (labels);
            tx.success();
            return all;
        }
    }
    
    public boolean _hasAnyLabels (String... labels) {
        for (String s : labels)
            if (_node.hasLabel(Label.label(s)))
                return true;
        return false;
    }

    public boolean hasAnyLabels (Label... labels) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            boolean any = _hasAnyLabels (labels);
            tx.success();
            return any;
        }
    }

    public boolean hasAnyLabels (String... labels) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            boolean any = _hasAnyLabels (labels);
            tx.success();
            return any;
        }
    }
    
    public boolean _is (Label label) {
        return _node.hasLabel(label);
    }

    public boolean _is (String label) {
        return _node.hasLabel(Label.label(label));
    }

    public boolean is (String label) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            boolean is = _is (label);
            tx.success();
            return is;
        }
    }

    public boolean is (Label label) {
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            boolean is = _is (label);
            tx.success();
            return is;
        }
    }
    
    public DataSource datasource () {
        DataSource source = null;
        try (Transaction tx = gdb.beginTx()) {
            if (_node.hasProperty(SOURCE)) {
                source = dsf.getDataSourceByKey
                    ((String)_node.getProperty(SOURCE));
            }
            tx.success();
        }
        return source;
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
            tx.success();
        }
    }
    
    public void _snapshot (String key, Object oldVal, Object newVal) {
        // update the timeline
        lastUpdated = System.currentTimeMillis();
        _node.setProperty(UPDATED, lastUpdated);
        /*
        Node snapshot = gdb.createNode(AuxNodeType.SNAPSHOT);
        snapshot.setProperty(PARENT, _node.getId());
        snapshot.setProperty(KEY, key);
        snapshot.setProperty(CREATED, lastUpdated);
        snapshot.setProperty(UPDATED, lastUpdated);
        if (oldVal != null)
            snapshot.setProperty(OLDVAL, oldVal);
        */
        if (newVal != null) {
            //snapshot.setProperty(NEWVAL, newVal);
            _node.setProperty(key, newVal);
        }
        else {
            _node.removeProperty(key);
        }
        timeline.add(_node, lastUpdated);
    }

    public Object get (String name) {
        Object value = null;
        try (Transaction tx = getGraphDb().beginTx()) {
            if (_node.hasProperty(name)) {
                value = _node.getProperty(name);
            }
            tx.success();
            return value;
        }
    }

    public Object _get (String name) {
        return _node.getProperty(name, null);
    }

    public Object removeProperty (String name) {
        Object value = null;
        try (Transaction tx = getGraphDb().beginTx()) {
            if (_node.hasProperty(name)) {
                value = _node.removeProperty(name);
            }
            tx.success();
            return value;
        }
    }

    public Object _removeProperty (String name) {
        return _node.removeProperty(name);
    }

    public void set (String name, Object value) {
        set (name, value, false);
    }
    
    public void set (String name, Object value, boolean index) {
        try (Transaction tx = getGraphDb().beginTx()) {
            if (_node.hasProperty(name)) {
                Object old = _node.getProperty(name);
                if (!value.equals(old)) {
                    _snapshot (name, old, value);
                    if (index) {
                        gdb.index().forNodes
                            (AuxNodeType.class.getName())
                            .add(_node, name, value);
                    }
                }
            }
            else {
                _snapshot (name, null, value);
                if (index) {
                    gdb.index().forNodes
                        (AuxNodeType.class.getName()).add(_node, name, value);
                }
            }
            tx.success();
        }
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
            rel.delete();
            if (xn.hasLabel(AuxNodeType.SNAPSHOT)
                || rel.isType(AuxRelType.PAYLOAD)) {
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
            boolean connected = connected (gdb.getNodeById(id));
            tx.success();
            return connected;
        }
    }

    public boolean connected (CNode n) {
        return connected (n._node);
    }

    protected static Node union (Node p, Node q) {
        Node P = getRoot (p);
        Node Q = getRoot (q);

        Node root = null;
        int rankp = (Integer)P.getProperty(RANK);
        int rankq = (Integer)Q.getProperty(RANK);
            
        long dif = rankp - rankq;
        if (dif == 0) {
            // if two ranks are the same, we designate the older
            // node as the parent
            dif = Q.getId() - P.getId();
        }
            
        if (dif < 0) { // P -> Q
            if (!P.equals(Q)) {
                P.setProperty(PARENT, Q.getId());
                P.removeLabel(AuxNodeType.COMPONENT);
                Q.setProperty(RANK, rankq+rankp);
            }
            root = Q;
        }
        else { // Q -> P
            if (!P.equals(Q)) {
                Q.setProperty(PARENT, P.getId());
                Q.removeLabel(AuxNodeType.COMPONENT);
                P.setProperty(RANK, rankq+rankp);
            }
            root = P;
        }
        
        return root;
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
            CNode node = _getLastUpdatedNode ();
            tx.success();
            return node;
        }
    }


    static protected Molecule getMol (Node node) {
        Molecule mol = null;
        try (Transaction tx = node.getGraphDatabase().beginTx()) {
            for (String f : MOLFIELDS) {
                String molfile = (String) node.getProperty(f, null);
                if (molfile != null) {
                    mol = Util.getMol(molfile);
                    if (mol != null)
                        break;
                }
            }
            tx.success();
        }
        return mol;
    }

    protected String getField (String name) {
        Relationship rel = _node.getSingleRelationship
            (AuxRelType.PAYLOAD, Direction.INCOMING);
        
        if (rel != null) {
            Node xn = rel.getOtherNode(_node);
            if (_node.hasProperty(SOURCE)) {
                DataSource ds =  dsf.getDataSourceByKey
                    ((String)_node.getProperty(SOURCE));
                String field = (String) ds._get(name);
                if (field != null) {
                    String value =
                        (String)xn.getProperty(field, null);
                    return value;
                }
            }
        }
        
        return null;
    }
    
    public Molecule mol () {
        Molecule mol = getMol (_node);
        if (mol == null) {      
            try (Transaction tx = gdb.beginTx()) {
                if (_node.hasLabel(AuxNodeType.ENTITY)) {
                    String molfile = getField ("StrucField");
                    if (molfile != null)
                        mol = Util.getMol(molfile);
                }
                tx.success();
            }
        }
        return mol;
    }

    public String name () {
        String name = "";
        try (Transaction tx = gdb.beginTx()) {
            Object value = _node.getProperty(NAME, null);
            if (value != null) {
                if (value.getClass().isArray())
                    value = Array.get(value, 0);
                name = (String)value;
            }
            else if (_node.hasLabel(AuxNodeType.ENTITY)) {
                name = getField ("NameField");
            }
            tx.success();
        }
        return name;
    }

    public String source () {
        String source = "";
        try (Transaction tx = gdb.beginTx()) {
            if (_node.hasLabel(AuxNodeType.DATA)) {
                Relationship rel = _node.getSingleRelationship
                    (AuxRelType.PAYLOAD, Direction.OUTGOING);
                if (rel != null) {
                    Node xn = rel.getOtherNode(_node);
                    DataSource ds = dsf.getDataSourceByKey
                        ((String)xn.getProperty(SOURCE, ""));
                    String field = (String) ds._get("IdField");
                    if (field != null) {
                        Object value = _node.getProperty(field, null);
                        if (value != null) {
                            if (value.getClass().isArray())
                                value = Array.get(value, 0);
                            source = (String) value;
                        }
                    }
                }
            }
            else {
                source = getField ("IdField");
            }
            tx.success();
        }
        return source;
    }
}
