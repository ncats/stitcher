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
            if (!node.hasLabel(AuxNodeType.SGROUP))
                throw new IllegalArgumentException
                    ("Node is not a stitch node!");
            return new Stitch (node);
        }
    }
    
    public static Stitch getStitch (CNode cnode) {
        return getStitch (cnode._node());
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
        if (ds != null) {
            String field = (String) ds.get(name);
            if (field != null) {
                try (Transaction tx = gdb.beginTx()) {
                    Object value = node.getProperty(field, null);
                    if (value != null && value.getClass().isArray())
                        value = Array.get(value, 0);
                    return (String)value;
                }
            }
        }
        return null;    
    }

    @Override
    public String name () {
        return (String) getField ("NameField", parent);
    }

    public String source () {
        try (Transaction tx = gdb.beginTx()) {
            return new CNode(parent).source();
        }
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
                return getMol (molfile);
        }
        return mol;
    }

    public Map[] members () {
        List<Map> mb = new ArrayList<>();
        for (Map.Entry<Node, DataSource> me : members.entrySet()) {
            Map m = new TreeMap ();
            m.put("id", me.getKey().getId());
            m.put("srcid", getField ("IdField", me.getKey()));
            m.put("name", getField ("NameField", me.getKey()));
            m.put("datasource", me.getValue().getName());
            mb.add(m);
        }
        return mb.toArray(new Map[0]);
    }
}
