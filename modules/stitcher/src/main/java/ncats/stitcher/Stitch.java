package ncats.stitcher;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import java.lang.reflect.Array;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.index.lucene.TimelineIndex;
import org.neo4j.graphdb.GraphDatabaseService;

import chemaxon.struc.Molecule;

public class Stitch extends Entity {
    static final Logger logger = Logger.getLogger(Stitch.class.getName());

    public static Stitch getStitch (Node node) {
        try (Transaction tx = node.getGraphDatabase().beginTx()) {
            Stitch s = _getStitch (node);
            tx.success();
            return s;
        }
    }

    public static Stitch _getStitch (Node node) {
        if (!node.hasLabel(AuxNodeType.SGROUP))
            throw new IllegalArgumentException
                ("Node is not a stitch node!");
        return new Stitch (node);
    }
    
    public static Stitch getStitch (CNode cnode) {
        return getStitch (cnode._node());
    }

    public static Stitch _getStitch (CNode cnode) {
        return _getStitch (cnode._node());
    }
    
    Node parent;
    Map<Node, DataSource> members = new HashMap<>();
    
    protected Stitch (Node node) {
        super (node);

        Long pid = (Long) node.getProperty(PARENT, null);
        for (Relationship rel : node.getRelationships
                 (AuxRelType.STITCH, Direction.OUTGOING)) {
            Node n = rel.getOtherNode(node); // this is payload node
            if (pid != null && n.getId() == pid) {
                parent = n;
            }

            Relationship payload = n.getSingleRelationship
                (AuxRelType.PAYLOAD, Direction.OUTGOING);
            if (payload != null) {
                Node pn = payload.getOtherNode(n);
                String source = (String) pn.getProperty(SOURCE, "");
                DataSource ds = dsf._getDataSourceByKey(source);
                members.put(n, ds);
            }
            else {
                logger.warning("Node "+n.getId()+" of stitch "+ node.getId()
                               +" has no data source!");
                members.put(n, null);
            }
        }

        if (members.isEmpty()) {
            throw new IllegalArgumentException
                ("Stitch node "+node.getId()+" has no members!");
        }
        
        if (parent == null) {
            logger.warning("Stitch node "+node.getId()+" has no parent!");
            parent = members.keySet().iterator().next();
        }
    }

    public int size () {
        Integer size = (Integer) get (RANK);
        return size != null ? size : -1;
    }

    String getField (String name, Node node) {
        DataSource ds = members.get(node);
        String val = null;
        if (ds != null) {
            String field = (String) ds.get(name);
            if (field == null)
                field = NAME;
            try (Transaction tx = gdb.beginTx()) {
                Object value = node.getProperty(field, null);
                if (value != null && value.getClass().isArray())
                    value = Array.get(value, 0);
                val = (String)value;
                tx.success();
            }
        }
        return val;
    }

    Map<String, Object> getProperties (Node node) {
        Map<String, Object> properties = new TreeMap<>();
        try (Transaction tx = gdb.beginTx()) {
            for (Map.Entry<String, Object> me
                     : node.getAllProperties().entrySet()) {
                Object value = properties.get(me.getKey());
                if (value != null)
                    properties.put
                        (me.getKey(), Util.merge(value, me.getValue()));
                else
                    properties.put(me.getKey(), me.getValue());
            }
        }
        return properties;
    }

    Map<String, Object> getStitches (Node node) {
        Map<String, Object> stitches = new TreeMap<>();
        try (Transaction tx = gdb.beginTx()) {
            // node is a DATA node
            Relationship rel = node.getSingleRelationship
                (AuxRelType.PAYLOAD, Direction.OUTGOING);
            if (rel != null) {
                Node xn = rel.getOtherNode(node);
                for (Relationship srel : xn.getRelationships
                         (EnumSet.allOf(StitchKey.class)
                          .toArray(new StitchKey[0]))) {
                    Object value = stitches.get(srel.getType().name());
                    if (value != null) {
                        value = Util.merge
                            (value, srel.getProperty("value"));
                    }
                    else
                        value = srel.getProperty("value");
                    stitches.put(srel.getType().name(), value);
                }
            }
        }
        return stitches;
    }

    @Override
    public String name () {
        return getField ("NameField", parent);
    }

    public String source () {
        try (Transaction tx = gdb.beginTx()) {
            String source = new CNode(parent).source();
            tx.success();
            return source;
        }
    }

    /*
    * stitch [1]->[n] payload -> entity
    * from the stitch node, we go through the payload to get to the
    * entity node. from there, we then iterate through all the payload
    * that connect to this entity and find those that match the given
    * source.
    */
    public List<Map<String, Object>> multiplePayloads (String source) {
        List<Map<String, Object>> payloads = new ArrayList<>();
        DataSource dsrc = dsf.getDataSource(source);
        if (dsrc == null) {
            throw new IllegalArgumentException
                    ("Not a valid data source: " + source);
        }
        try (Transaction tx = gdb.beginTx()) {
            for (Relationship rel : _node.getRelationships
                    (Direction.BOTH, AuxRelType.STITCH)) {
                Node n = rel.getOtherNode(_node); // payload
                DataSource ds = dsf._getDataSource
                        ((String)rel.getProperty(SOURCE, null));
                if (dsrc.equals(ds)) {

                    payloads.add(new TreeMap<>(n.getAllProperties()));
                }
                else {
                    Node e = n.getSingleRelationship
                            (AuxRelType.PAYLOAD, Direction.OUTGOING)
                            .getOtherNode(n);
                    for (Relationship prel : e.getRelationships
                            (Direction.INCOMING, AuxRelType.PAYLOAD)) {
                        Node p = prel.getOtherNode(e);
                        ds = dsf._getDataSource
                                ((String)prel.getProperty(SOURCE, null));
                        if (dsrc.equals(ds)) {

                            payloads.add(new TreeMap<>(p.getAllProperties()));
                        }
                    }
                }
            }
            tx.success();
        }
        return payloads;
    }

    /*
     * stitch [1]->[n] payload -> entity
     * from the stitch node, we go through the payload to get to the
     * entity node. from there, we then iterate through all the payload
     * that connect to this entity and find those that match the given
     * source.
     */
    public Map<String, Object> payload (String source) {
       Map<String, Object> merged = new TreeMap<>();
       for(Map<String, Object> p : multiplePayloads(source)){
           merged.putAll(p);
       }
       return merged;
    }

    public Map<DataSource, Integer> datasources () {
        Map<DataSource, Integer> dsources = new TreeMap<>();
        for (Map.Entry<Node, DataSource> me : members.entrySet()) {
            Integer c = dsources.get(me.getValue());
            dsources.put(me.getValue(), c==null ? 1 :c+1);
        }
        return dsources;
    }

    @Override
    public Molecule mol () {
        Molecule mol = getMol (parent);
        if (mol == null) {
            String molfile = (String) getField ("StrucField", parent);
            if (molfile != null)
                return Util.getMol(molfile);
        }
        return mol;
    }

    public Map[] members () {
        List<Map> mb = new ArrayList<>();
        for (Map.Entry<Node, DataSource> me : members.entrySet()) {
            Map m = new TreeMap ();
            m.put("id", me.getKey().getId());
            long parent = 0;
            try (Transaction tx = gdb.beginTx()) {
                for (Relationship rel: me.getKey().getRelationships(Direction.BOTH, AuxRelType.PAYLOAD))
                    parent = rel.getOtherNode(me.getKey()).getId();
                tx.success();
            }
            m.put("parent", parent);
            m.put("srcid", getField (DataSource.IDFIELD, me.getKey()));
            m.put("name", getField (DataSource.NAMEFIELD, me.getKey()));
            m.put("datasource", me.getValue().getName());
            m.put("properties", getProperties (me.getKey()));
            m.put("stitches", getStitches (me.getKey()));
            mb.add(m);
        }
        return mb.toArray(new Map[0]);
    }
}
