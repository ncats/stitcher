package serializer;

import java.util.*;
import java.lang.reflect.Array;

import javax.inject.Inject;
import play.libs.Json;
import play.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.GraphDatabaseService;

import ncats.stitcher.*;
import services.EntityService;

public class DefaultJsonCodec implements JsonCodec, Props {
    final ObjectMapper mapper = Json.mapper();
    final EntityService es;

    @Inject
    public DefaultJsonCodec (EntityService es) {
        this.es = es;
        Logger.debug("$$ "+getClass()+" initialized!");
    }

    public JsonNode encode (CNode node) {
        Node _node = node._node();
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            JsonNode json = toJson (_node);
            tx.success();
            return json;
        }
    }

    DataSource datasource (Node _node) {
        DataSource source = null;
        try (Transaction tx = _node.getGraphDatabase().beginTx()) {
            if (_node.hasProperty(SOURCE)) {
                source = es.getDataSourceFactory().getDataSourceByKey
                    ((String)_node.getProperty(SOURCE));
            }
            tx.success();
        }
        return source;
    }

    Set<String> _labels (Node _node) {
        Set<String> labels = new TreeSet<String>();
        for (Label l : _node.getLabels()) {
            labels.add(l.name());
        }
        return labels;
    }

    
    JsonNode toJson (Node _node) {
        ObjectNode node = mapper.createObjectNode();
        
        for (Map.Entry<String, Object> me :
                 _node.getAllProperties().entrySet()) {
            Util.setJson(node, me.getKey(), me.getValue());
        }
        node.put("id", _node.getId());  

        DataSource ds = datasource (_node);
        if (ds != null) {
            ObjectNode src = mapper.createObjectNode();
            src.put("id", ds.getId());
            src.put("key", ds.getKey());
            src.put("name", ds.getName());
            node.put("datasource", src);
        }

        ArrayNode array = mapper.createArrayNode();
        for (String l : _labels (_node)) {
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
        Set<ObjectNode> members = new TreeSet<>((a,b) -> {
                long x = a.get("node").asLong(), y = b.get("node").asLong();
                if (x < y) return -1;
                if (x > y) return 1;
                return 0;
            });
        
        if (_node.hasLabel(AuxNodeType.SGROUP)) {
            stitches = mapper.createObjectNode();
            stitches.put("hash", (String) _node.getProperty(ID, null));
            stitches.put("size", (Integer)_node.getProperty(RANK, 0));
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
                    ds = es.getDataSourceFactory().getDataSourceByKey(src);
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
                        Logger.warn("Unknown data source: "+src);
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
                            on.put(SOURCE, es.getDataSourceFactory()
                                   .getDataSourceByKey(source).getName());
                            data.add(on);
                        }
                    }
                    
                    if (data.size() > 0)
                        member.put("data", data);
                }
                
                members.add(member);
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

        if (!members.isEmpty())
            stitches.put("members", mapper.valueToTree(members));

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
}
