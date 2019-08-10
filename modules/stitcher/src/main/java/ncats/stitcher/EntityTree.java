package ncats.stitcher;

import java.util.*;
import java.util.stream.Collectors;
import org.neo4j.graphdb.Node;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

public class EntityTree implements Iterable<Entity> {
    public static class ENode implements Iterable<ENode> {
        final Node node;
        
        public ENode parent;
        public final List<ENode> children = new ArrayList<>();
        final ObjectMapper mapper = new ObjectMapper ();
        
        ENode (Node node) {
            if (node == null)
                throw new IllegalArgumentException ("Node can't be null");
            this.node = node;
        }

        ENode add (ENode node) {
            if (node.parent != null)
                node.parent.children.remove(node);
            node.parent = this;
            children.add(node);
            return this;
        }

        public int hashCode () { return node.hashCode(); }
        public boolean equals (Object obj) {
            if (obj instanceof ENode) {
                return node.equals(((ENode)obj).node);
            }
            return false;
        }

        ENode find (Node n) {
            return find (this, n);
        }
        
        public Entity entity () { return Entity.getEntity(node); }
        
        public Iterator<ENode> iterator () {
            Set<ENode> visited = new LinkedHashSet<>();
            dfs (this, visited);
            return visited.iterator();
        }
        
        public ENode[] toArray () {
            Set<ENode> visited = new LinkedHashSet<>();
            dfs (this, visited);
            return visited.toArray(new ENode[0]);
        }

        static void dfs (ENode node, Set<ENode> visited) {
            visited.add(node);
            for (ENode n : node.children)
                dfs (n, visited);
        }

        static void find (List<ENode> found, ENode node, Node n) {
            if (node.node.equals(n))
                found.add(node);
            else {
                for (ENode child : node.children)
                    find (found, child, n);
            }
        }
        
        static ENode find (ENode node, Node n) {
            List<ENode> found = new ArrayList<>();
            find (found, node, n);
            return found.isEmpty() ? null : found.get(0);
        }

        public JsonNode toJson (String... fields) {
            ObjectNode json = mapper.createObjectNode();
            toJson (json, this, fields);
            return json;
        }

        void toJson (ObjectNode json, ENode node, String... fields) {
            json.put("node", node.node.getId());
            Entity e = node.entity();

            if (fields != null) {
                for (String f: fields) {
                    Object v = e.payload(f);
                    if (v != null)
                        json.put(f, mapper.valueToTree(v));
                }
            }
            
            ArrayNode children = mapper.createArrayNode();
            for (ENode n : node.children) {
                ObjectNode child = mapper.createObjectNode();
                toJson (child, n, fields);
                children.add(child);
            }
            
            if (children.size() > 0)
                json.put("children", children);
        }
    }

    public final ENode root;
    EntityTree (Node... nodes) {
        if (nodes.length == 0)
            throw new IllegalArgumentException ("Empty nodes!");
        ENode node = root = new ENode (nodes[0]);
        for (int i = 1; i < nodes.length; ++i) {
            ENode n = new ENode (nodes[i]);
            node.add(n);
            node = n;
        }
    }

    void add (Node... path) {
        ENode node = root;
        int i = 0;
        for (; i < path.length; ++i) {
            ENode n = node.find(path[i]);
            if (n == null)
                break;
            node = n;
        }

        for (int j = i; j < path.length; ++j) {
            ENode n = new ENode (path[j]);
            node.add(n);
            node = n;
        }
    }
    
    public Iterator<Entity> iterator () {
        Set<ENode> visited = new LinkedHashSet<>();
        dfs (root, visited);
        return visited.stream().map(e -> e.entity())
            .collect(Collectors.toList()).iterator();
    }

    static void dfs (ENode node, Set<ENode> visited) {
        visited.add(node);
        for (ENode child : node.children)
            dfs (child, visited);
    }
}
