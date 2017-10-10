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
            Set<String> labels = _labels ();
            tx.success();
            return labels;
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
            CNode node = _getLastUpdatedNode ();
            tx.success();
            return node;
        }
    }

    public JsonNode toJson () {
        try (Transaction tx = gdb.beginTx()) {
            JsonNode json = _toJson ();
            tx.success();
            return json;
        }
    }
    
    public JsonNode _toJson () {
        ObjectNode node = mapper.createObjectNode();
        
        for (Map.Entry<String, Object> me :
                 _node.getAllProperties().entrySet()) {
            Util.setJson(node, me.getKey(), me.getValue());
        }
        node.put("id", _node.getId());  

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
        
        ObjectNode stitches = null;
        if (_node.hasLabel(AuxNodeType.SGROUP)) {
            stitches = mapper.createObjectNode();
            stitches.put("hash", (String) _node.getProperty(ID, null));
            stitches.put("size", (Integer)_node.getProperty(RANK, 0));
            stitches.put("members", mapper.createArrayNode());
            stitches.put("parent", (Long)_node.getProperty(PARENT, null));
            node.put("sgroup", stitches);
        }

        Map<String, Object> properties = new TreeMap<>();
        Map<Object, Long> refs = new HashMap<>();
        List<JsonNode> payloads = new ArrayList<>();
        List<JsonNode> events = new ArrayList<>();
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
            else if (rel.isType(AuxRelType.STITCH)) {
                ObjectNode member = mapper.createObjectNode();
                member.put("node", n.getId());
                member.put(SOURCE, (String)rel.getProperty(SOURCE));
                                
                for (Map.Entry<String, Object> me
                         : n.getAllProperties().entrySet()) {
                    if (me.getValue().getClass().isArray()) {
                        int len = Array.getLength(me.getValue());
                        for (int i = 0; i < len; ++i)
                            refs.put(Array.get(me.getValue(), i), n.getId());
                    }
                    else {
                        refs.put(me.getValue(), n.getId());
                    }
                    
                    Object value = properties.get(me.getKey());
                    if (value != null)
                        properties.put
                            (me.getKey(), Util.merge(value, me.getValue()));
                    else
                        properties.put(me.getKey(), me.getValue());
                }
                
                Relationship payrel = n.getSingleRelationship
                    (AuxRelType.PAYLOAD, Direction.OUTGOING);
                if (payrel != null) {
                    Node sn = payrel.getOtherNode(n);
                    
                    String src = (String) sn.getProperty(SOURCE, null);
                    ds = dsf.getDataSourceByKey(src);
                    if (ds != null) {
                        String field = (String) ds.get("IdField");
                        if (field != null) {
                            Object val = n.getProperty(field, null);
                            if (val != null) {
                                if (val.getClass().isArray())
                                    val = Array.get(val, 0);
                                member.put("id", mapper.valueToTree(val));
                            }
                        }
                        
                        field = (String) ds.get("NameField");
                        if (field != null) {
                            Object val = n.getProperty(field, null);
                            if (val != null) {
                                if (val.getClass().isArray())
                                    val = Array.get(val, 0);
                                member.put("name", mapper.valueToTree(val));
                            }
                        }
                    }
                    else {
                        logger.warning("Unknown data source: "+src);
                    }
                    
                    Map<String, Object> map = new TreeMap<>();
                    for (Relationship srel : sn.getRelationships
                             (EnumSet.allOf(StitchKey.class)
                              .toArray(new StitchKey[0]))) {
                        Object value = map.get(srel.getType().name());
                        if (value != null) {
                            value = Util.merge
                                (value, srel.getProperty("value"));
                        }
                        else
                            value = srel.getProperty("value");
                        map.put(srel.getType().name(), value);
                    }
                    member.put("stitches", mapper.valueToTree(map));

                    ArrayNode data = mapper.createArrayNode();
                    for (Relationship srel :
                             sn.getRelationships(AuxRelType.PAYLOAD)) {
                        Node py = srel.getOtherNode(sn);
                        if (!py.equals(n)) {
                            ObjectNode on = Util.toJsonNode(py);
                            String source = (String)srel.getProperty(SOURCE);
                            on.put(SOURCE, dsf.getDataSourceByKey
                                   (source).getName());
                            data.add(on);
                        }
                    }
                    
                    if (data.size() > 0)
                        member.put("data", data);
                }
                
                ((ArrayNode)stitches.get("members")).add(member);               
            }
            else if (rel.isType(AuxRelType.PAYLOAD)) {
                ObjectNode on = Util.toJsonNode(rel);
                Util.toJsonNode(on, n);
                payloads.add(on);
            }
            else if (rel.isType(AuxRelType.EVENT)) {
                ObjectNode on = Util.toJsonNode(rel);
                Util.toJsonNode(on, n);
                events.add(on);
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

        if (!properties.isEmpty()) {
            for (Map.Entry<String, Object> me : properties.entrySet()) {
                Object value = me.getValue();
                if (value.getClass().isArray()) {
                    int len = Array.getLength(value);
                    Object[] vals = new Object[len];
                    for (int i = 0; i < len; ++i) {
                        Object v = Array.get(value, i);
                        Map m = new TreeMap ();
                        m.put("value", v);
                        m.put("node", refs.get(v));
                        vals[i] = m;
                    }
                    value = vals;
                }
                else {
                    Map m = new TreeMap ();
                    m.put("value", value);
                    m.put("node", refs.get(value));
                    value = m;
                }
                me.setValue(value);
            }
            
            stitches.put("properties", mapper.valueToTree(properties));
        }
            
        if (neighbors.size() > 0)
            node.put("neighbors", neighbors);
            
        if (!payloads.isEmpty()) {
            ArrayNode ary = mapper.createArrayNode();
            for (JsonNode n : payloads) {
                ary.add(n);
            }
            node.put("payload", ary);
        }

        if (!events.isEmpty()) {
            ArrayNode ary = mapper.createArrayNode();
            for (JsonNode n : events) {
                ary.add(n);
            }
            node.put("events", ary);        
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

    static protected Molecule getMol (String molfile) {
        try {
            MolHandler mh = new MolHandler (molfile);
            Molecule mol = mh.getMolecule();
            if (mol.getDim() < 2)
                mol.clean(2, null);
            return mol;
        }
        catch (Exception ex) {
            ex.printStackTrace();
            logger.warning("Not a valid molfile: "+molfile);
        }
        return null;
    }

    static protected Molecule getMol (Node node) {
        Molecule mol = null;
        try (Transaction tx = node.getGraphDatabase().beginTx()) {
            String molfile = (String) node.getProperty("MOLFILE", null);
            if (molfile != null)
                mol = getMol (molfile);
            else {
                molfile = (String) node.getProperty("SMILES", null);
                if (molfile != null)
                    mol = getMol (molfile);
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
                        mol = getMol (molfile);
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
