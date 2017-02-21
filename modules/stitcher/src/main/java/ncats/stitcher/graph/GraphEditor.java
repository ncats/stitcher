package ncats.stitcher.graph;

import chemaxon.formats.MolImporter;
import ncats.stitcher.Entity;
import ncats.stitcher.EntityType;
import ncats.stitcher.PredicateType;
import ncats.stitcher.StitchKey;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;
import java.util.logging.Logger;

import ncats.stitcher.AuxRelType;

/**
 * Created by southalln on 11/28/15.
 */
public class GraphEditor {
    static final Logger logger = Logger.getLogger
            (GraphEditor.class.getName());

    protected final GraphDatabaseService gdb;

    public GraphEditor (GraphDatabaseService gdb) {
        this.gdb = gdb;
    }

    public void deprecateMetaLabel(Label source, RelationshipType type, boolean strict, int limit, boolean test) {
        try (Transaction tx = gdb.beginTx()) {
            HashMap<Integer,Integer> sc = new HashMap<>(); // histogram, counts of counts
            sc.put(0,0);
            HashSet<String> ccVisited = new HashSet<>(); // values already evaluated

            ResourceIterator<Node>
                    nodes = gdb.findNodes(source);
            while (nodes.hasNext()) {
                Node node = nodes.next();
                HashMap<String,Integer> v = new HashMap<>(); // value : counts
                ArrayList<Long> on = new ArrayList<>(); // nodes linked by relationships other than this type
                for (Relationship r: node.getRelationships()) {
                    if (!r.isType(type) && (strict || !r.isType(StitchKey.H_LyChI_L3))) {
                        if (!on.contains(r.getOtherNode(node).getId()))
                            on.add(r.getOtherNode(node).getId());
                    }
                }
                for (Relationship r: node.getRelationships(type)) {
                    String val = r.getProperty("_value").toString();
                    if (!ccVisited.contains(val) // value we haven't seen yet
                            && (!strict || r.getOtherNode(node).hasLabel(source)) // connected to node from same source
                            && !on.contains(r.getOtherNode(node).getId())) { // connected to a node only connected through this type
                        if (!v.containsKey(val)) v.put(val, 0);
                        v.put(val, v.get(val) + 1);
                    }
                }
                if (node.hasProperty(type.name())) {
                    Object value = node.getProperty(type.name());
                    if (value.getClass().isArray()) {
                        for (String prop : (String[]) value)
                            if (!v.containsKey(prop))
                                sc.put(0, sc.get(0) + 1);
                    } else if (!v.containsKey(value)) {
                        sc.put(0, sc.get(0) + 1);
                    }
                }
                for (String key: v.keySet()) {
                    ccVisited.add(key);
                    Integer scKey = v.get(key);
                    if (!sc.containsKey(scKey)) sc.put(scKey, 0);
                    sc.put(scKey, sc.get(scKey)+1);
                    if (scKey > limit) {
                        if (!test) {
                            // deprecate prop value
                            Index<Node> index = node.getGraphDatabase().index()
                                   .forNodes(type.name()+".node_index");
                            IndexHits<Node> hits = index.get(type.name(), key);
                            for (Node hit: hits)
                                if (hit.hasLabel(source))
                                    Entity._deprecateProperty(hit, type, key);
                            hits.close();
                        } else {
                            System.out.println("Deprecate  "+node.getId()+":"+type.name()+":"+key);
                        }
                    }
                }
            }
            tx.success();

            for (int i=0; i<100; i++) {
                if (sc.containsKey(i))
                    System.out.println(i+" source nodes in cc: "+sc.get(i));
            }
            int count = 0;
            for (int key: sc.keySet())
                if (key > 100) count = count + sc.get(key);
            System.out.println(">100 source nodes in cc: "+count);
        }
    }

    class Graph {
        public Graph(Label cc, List<String> relationshipTypes) {
            ResourceIterator<Node> ccNodes = gdb.findNodes(cc);
            while (ccNodes.hasNext()) {
                Node ccNode = ccNodes.next();
                add(ccNode);
                for (Relationship r : ccNode.getRelationships())
                    if (relationshipTypes.contains(r.getType().name()))
                        add(r);
            }
        }
        public HashSet<Node> n = new HashSet<>();
        HashSet<Relationship> e = new HashSet<>();
        HashMap<Long, Integer> c = new HashMap<>(); // color nodes
        HashMap<Integer, Set<Node>> cs = new HashMap<>();
        public void add(Node n) {this.n.add(n); c.put(n.getId(), 0);}
        public void add(Relationship r) {this.e.add(r);}

        private ArrayList<Node> step(ArrayList<ArrayList<Node>> paths, Node end) {
            ArrayList<ArrayList<Node>> newPaths = new ArrayList<>();
            HashSet<Node> added = new HashSet<>();
            for (ArrayList<Node> path: paths) {
                Node last = path.get(path.size()-1);
                for (Relationship r: connect(last)) {
                    if (!path.contains(r.getOtherNode(last))
                            && !added.contains(r.getOtherNode(last))) {
                        ArrayList<Node> newPath = new ArrayList<>();
                        for (Node n: path) newPath.add(n);
                        newPath.add(r.getOtherNode(last));
                        newPaths.add(newPath);
                        added.add(r.getOtherNode(last));
                    }
                }
            }
            if (newPaths.size() == 0)
                return null;
            for (ArrayList<Node> path: newPaths)
                if (path.get(path.size()-1).equals(end))
                    return path;
            return step(newPaths, end);
        }
        public ArrayList<Node> shortestPath(Node n1, Node n2) {
            ArrayList<Node> path = new ArrayList<>();
            path.add(n1);
            ArrayList<ArrayList<Node>> paths = new ArrayList<>();
            paths.add(path);
            return step(paths, n2);
        }
        public Set<Relationship> connect(Node n1) {
            return connect(n1, null);
        }
        public Set<Relationship> connect(Node n1, Node n2) {
            HashSet<Relationship> rs = new HashSet<>();
            for (Relationship r: e)
                if (r.getStartNode().getId() == n1.getId() || r.getEndNode().getId() == n1.getId())
                    if (n2==null || r.getOtherNode(n1).getId() == n2.getId())
                        rs.add(r);
            return rs;
        }
        public int size(Label source) { return size(null, source); }
        public int size(ArrayList<Node> path, Label source) {
            int size = 0;
            for (Node node: (path == null ? n : path)) if (node.hasLabel(source)) size++;
            return size;
        }
        public void color() {
            int color = 1;
            Set<Node> rem = new HashSet<>();
            for (Node node: n) rem.add(node);
            while(rem.size() > 0) {
                cs.put(color, new HashSet<Node>());
                Set<Node> layer = new HashSet<>();
                Node node = rem.iterator().next();
                layer.add(node);
                rem.remove(node);
                while (layer.size() > 0) {
                    Set<Node> newLayer = new HashSet<>();
                    for (Node lNode : layer) {
                        for (Relationship rel : connect(lNode)) {
                            if (rem.contains(rel.getOtherNode(lNode))) {
                                newLayer.add(rel.getOtherNode(lNode));
                                rem.remove(rel.getOtherNode(lNode));
                            }
                        }
                        c.put(lNode.getId(), color);
                        cs.get(color).add(lNode);
                    }
                    layer = newLayer;
                }
                color++;
            }
        }
        public void update(Node anchor) {
            ArrayList<Node> ns = new ArrayList<>();
            ArrayList<Node> added = new ArrayList<>();
            added.add(anchor);
            while (added.size() > 0) {
                for (Node add : added)
                    if (!ns.contains(add))
                        ns.add(add);
                added = new ArrayList<>();
                for (Node add : ns)
                    for (Relationship r : connect(add))
                        if (!ns.contains(r.getOtherNode(add)))
                            added.add(r.getOtherNode(add));
            }
            HashSet<Node> rem = new HashSet<>();
            for (Node node: n)
                if (!ns.contains(node))
                    rem.add(node);
            for (Node node: rem)
                n.remove(node);
        }
    }

    public void chooseClosestSource(EntityType type, Label source, List<String> relationshipTypes) {
        try (Transaction tx = gdb.beginTx()) {
            Label ccNull = DynamicLabel.label("CC_null");

            ResourceIterator<Node> nodes = gdb.findNodes(type);
            while (nodes.hasNext()) {
                Node node = nodes.next();
                Label cc = ccNull;
                for (Label l : node.getLabels())
                    if (l.name().startsWith("CC_"))
                        cc = l;
                if (!node.hasLabel(source) && !cc.name().equals(ccNull.name())) {
                    HashMap<Long, int[]> counts = new HashMap<>();
                    for (Relationship r: node.getRelationships()) {
                        Node on = r.getOtherNode(node);
                        if (on.hasLabel(source) && relationshipTypes.contains(r.getType().name())) {
                            if (!counts.containsKey(on.getId()))
                                counts.put(on.getId(), new int[relationshipTypes.size()]);
                            counts.get(on.getId())[relationshipTypes.indexOf(r.getType().name())]++;
                        }
                    }
                    Long bestMatch = 0L;
                    for (Long nodeId: counts.keySet()) {
                        if (bestMatch == 0L) bestMatch = nodeId;
                        else {
                            for (int i=0; i<relationshipTypes.size(); i++) {
                                if (counts.get(nodeId)[i] > counts.get(bestMatch)[i]) {
                                    bestMatch = nodeId;
                                    break;
                                } else if (counts.get(nodeId)[i] < counts.get(bestMatch)[i]) {
                                    break;
                                }
                            }
                        }
                    }
                    if (bestMatch != 0L) {
                        HashSet<String> goodValues = new HashSet<>();
                        for (Relationship r: node.getRelationships()) {
                            Node on = r.getOtherNode(node);
                            if (on.getId() == bestMatch && r.hasProperty("_value"))
                                goodValues.add(r.getProperty("_value").toString());
                        }
                        HashSet<Map.Entry<RelationshipType, String>> badValues = new HashSet<>();
                        for (Relationship r: node.getRelationships()) {
                            Node on = r.getOtherNode(node);
                            if (on.hasLabel(source) && on.getId() != bestMatch && r.hasProperty("_value")) {
                                String v = r.getProperty("_value").toString();
                                if (goodValues.contains(v)) {
                                    // deprecate relationship
                                    Entity._delete(r);
                                } else {
                                    badValues.add(new AbstractMap.SimpleEntry(r.getType(), v));
                                }
                            }
                        }
                        // deprecate badValues
                        for (Map.Entry<RelationshipType, String> me: badValues)
                            Entity._deprecateProperty(node, me.getKey(), me.getValue());
                    }
                }
            }
            tx.success();
        }
    }

    class Color {
        int color = 1;
        HashMap<Long, Integer> cMap = new HashMap<>();
        HashMap<Integer,Set<Long>> mapC = new HashMap<>();

        public void update(Set<Integer> cMerge, Set<Long> nMerge) {
            // find color to use
            int mycolor = color;
            for (int c : cMerge) {
                if (c < color) mycolor = c;
            }
            // reassign sets
            for (int c : cMerge) {
                for (long node : mapC.get(c)) {
                    nMerge.add(node);
                }
                mapC.remove(c);
            }
            // assign new nodes
            for (Long node : nMerge) {
                cMap.put(node, mycolor);
            }
            mapC.put(mycolor, nMerge);
            if (mycolor == color)
                color++;
        }
    }

    public void greedyConnectedComponents(Label source, List<String> relationshipTypes) {
        try (Transaction tx = gdb.beginTx()) {
            Color c = new Color();

            // first by PredicateType
            PredicateType[] pts = {PredicateType.ConceptOf, PredicateType.ActiveMoiety};
            ResourceIterator<Node> nodes = gdb.findNodes(source);
            while (nodes.hasNext()) {
                Node n = nodes.next();
                HashSet<Integer> cMerge = new HashSet<Integer>(); // color labels to merge
                HashSet<Long> nMerge = new HashSet<Long>(); // nodes to make the same color
                nMerge.add(n.getId());
                if (c.cMap.containsKey(n.getId())) cMerge.add(c.cMap.get(n.getId()));

                for (Relationship r : n.getRelationships()) {
                    for (PredicateType pt : pts) {
                        if (r.isType(pt)) {
                            long on = r.getOtherNode(n).getId();
                            if (c.cMap.containsKey(on)) { // make use of previously assigned color set
                                cMerge.add(c.cMap.get(on));
                            } else { // assign node not yet visited same color as this node
                                nMerge.add(on);
                            }
                        }
                    }
                }
                c.update(cMerge, nMerge);
            }

            // then growing outward
            boolean repeat = true;
            while (repeat) {
                repeat = false;
                nodes = gdb.findNodes(EntityType.Agent);
                while (nodes.hasNext()) {
                    Node n = nodes.next();
                    if (!c.cMap.containsKey(n.getId())) {
                        HashMap<Long, int[]> links = new HashMap<>();
                        for (Relationship r : n.getRelationships()) {
                            long on = r.getOtherNode(n).getId();
                            RelationshipType rt = r.getType();
                            if (c.cMap.containsKey(on) && relationshipTypes.contains(rt.name())) {
                                if (!links.containsKey(on)) {
                                    links.put(on, new int[relationshipTypes.size()]);
                                }
                                int index = relationshipTypes.indexOf(r.getType().name());
                                int[] link = links.get(on);
                                link[index] = link[index]+1;
                                links.put(on, link);
                            }
                        }
                        if (links.size() > 0) {
                            repeat = true;
                            long best = 0;
                            for (long link: links.keySet()) {
                                if (best == 0) {
                                    best = link;
                                }
                                for (int i=0; i<relationshipTypes.size(); i++) {
                                    if (links.get(link)[i] > links.get(best)[i]) {
                                        best = link;
                                        break;
                                    } else if (links.get(link)[i] < links.get(best)[i]) {
                                        break;
                                    }
                                }
                            }
                            c.cMap.put(n.getId(), c.cMap.get(best));
                            c.mapC.get(c.cMap.get(best)).add(n.getId());
                        }
                    }
                }
            }

            // finally, across all agents
            nodes = gdb.findNodes(EntityType.Agent);
            while (nodes.hasNext()) {
                Node n = nodes.next();
                HashSet<Integer> cMerge = new HashSet<Integer>(); // color labels to merge
                HashSet<Long> nMerge = new HashSet<Long>(); // nodes to make the same color
                if (!c.cMap.containsKey(n.getId())) {
                    nMerge.add(n.getId());
                    for (Relationship r : n.getRelationships()) {
                        RelationshipType rt = r.getType();
                        if (relationshipTypes.contains(rt.name())) {
                            long on = r.getOtherNode(n).getId();
                            if (c.cMap.containsKey(on)) { // make use of previously assigned color set
                                cMerge.add(c.cMap.get(on));
                            } else { // assign node not yet visited same color as this node
                                nMerge.add(on);
                            }
                        }
                    }
                    c.update(cMerge, nMerge);
                }
            }

            // change node labels
            for (long nId: c.cMap.keySet()) {
                Node n = gdb.getNodeById(nId);
                Label old = null;
                for (Label l: n.getLabels()) {
                    if (l.name().startsWith("CC_"))
                        old = l;
                }
                if (old != null)
                    n.removeLabel(old);
                n.addLabel(DynamicLabel.label("CC_"+c.cMap.get(nId)));
            }
            tx.success();
        }
    }

    public void writeOutNodeTags(String filename) throws IOException {
        String[] labelSet = {
                "npc-dump",
                "ginas",
                "integr",
                "HTS amenable drugs",
                "Human approved drugs",
                "FDA human approved",
                "FDA DailyMed",
                "FDA drugs@FDA",
                "FDA NDC",
                "FDA orange book",
                "ORANGE BOOK",
                "FDA OTC",
                "DRUGS@FDA",
                "NDF-RT",
                "NF",
                "EMA human approved",
                "EMA",
                "EMA EPAR",
                "Canada",
                "EVMPD",
                "Japan",
                "SWISS MEDIC",
                "UK NHS",
                "WHO essential",
                "Approved drugs",
                "FDA approved",
                "EMA HERBAL SUBSTANCE",
                "EMA orphan",
                "EMA veterinary approved",
                "FDA green book",
                "GREEN BOOK",
                "FDA maximum daily dose",
                "BP",
                "EP",
                "JP",
                "BAN",
                "INN",
                "JAN",
                "MERCK",
                "MERCK INDEX",
                "MI",
                "MONOCLONAL",
                "USAN",
                "USP",
                "USP-MC",
                "USP-RC",
                "USP-RS",
                "US DEA",
                "VAN",
                "VANDF",
                "MART.",
                "MARTINDALE",
                "WHO-DD",
                "GRAS",
                "INCI",
                "JECFA",
                "PCPC",
                "PCPC-DB",
                "CFSAN",
                "FHFI",
                "II",
                "ALANWOOD.NET"
        };

        try (Transaction tx = gdb.beginTx()) {
            RelationshipType payload = AuxRelType.PAYLOAD;

            PrintStream fos = new PrintStream(new FileOutputStream(filename));
            Set<Long> visited = new HashSet<>();
            ArrayList<String> labels = new ArrayList<>(Arrays.asList(labelSet));
            StringBuffer sb = new StringBuffer();
            sb.append("IDs\tName\tLyChI\tsmiles");
            for (String entry: labels)
                sb.append("\t"+entry);
            for (Label l: gdb.getAllLabels())
                if (!l.name().startsWith("CC_") && !labels.contains(l.name())) {
                    String lName = l.name();
                    if (lName.indexOf('-') > -1 && new Scanner(lName.substring(lName.indexOf('-')+1)).hasNextInt())
                        lName = lName.substring(0, lName.indexOf('-'));
                    if (!labels.contains(lName)) {
                        labels.add(lName);
                        sb.append("\t"+lName);
                    }
                }
            sb.append("\n");
            fos.print(sb.toString());

            MolImporter mi = new MolImporter();
            ResourceIterator<Node> nodes = gdb.findNodes(EntityType.Agent);
            while (nodes.hasNext()) {
                Node node = nodes.next();
                if (visited.contains(node.getId()))
                    continue;
                Label cc = null;
                for (Label l : node.getLabels())
                    if (l.name().startsWith("CC_"))
                        cc = l;
                if (cc != null) {
                    ArrayList<Node> ccNodes = new ArrayList<>();
                    StringBuffer ccIDs = new StringBuffer();
                    String ccName = "";
                    String lychi = "";
                    String smiles = "";
                    int[] ccLabels = new int[labels.size()];
                    ResourceIterator<Node> nodeSet = gdb.findNodes(cc);
                    while (nodeSet.hasNext()) {
                        Node ccNode = nodeSet.next();
                        for (Label l: ccNode.getLabels()) {
                            String lName = l.name();
                            if (lName.indexOf('-') > -1 && new Scanner(lName.substring(lName.indexOf('-')+1)).hasNextInt())
                                lName = lName.substring(0, lName.indexOf('-'));
                            if (labels.contains(lName))
                                ccLabels[labels.indexOf(lName)] = ccLabels[labels.indexOf(lName)] + 1;
                        }
                        ccNodes.add(ccNode);
                        visited.add(ccNode.getId());
                        if (ccIDs.length() > 0) ccIDs.append(";");
                        ccIDs.append(ccNode.getProperty("_ID").toString());
                        if (ccName.length() == 0 && ccNode.hasProperty("N_Name") && ccNode.hasLabel(DynamicLabel.label("integr"))) {
                            for (int i=0; i<Array.getLength(ccNode.getProperty("N_Name")); i++) {
                                String nName = Array.get(ccNode.getProperty("N_Name"), 0).toString().trim();
                                if (nName.length() > ccName.length()) ccName = nName;
                            }
                        }
                        if (smiles.length() == 0) {
                            Relationship rel = ccNode.getSingleRelationship(payload, Direction.INCOMING);
                            Node pn = rel.getOtherNode(ccNode);
                            if (pn.hasProperty("MOLFILE")) {
                                try {
                                    smiles = mi.importMol(pn.getProperty("MOLFILE").toString()).toFormat("smiles");
                                } catch (Exception e) {}
                            }
                        }
                        if (ccName.length() == 0) {
                            Relationship rel = ccNode.getSingleRelationship(payload, Direction.INCOMING);
                            Node pn = rel.getOtherNode(ccNode);
                            if (pn.hasProperty("name")) {
                                if (pn.getProperty("name").getClass().isArray())
                                    ccName = Array.get(pn.getProperty("name"), 0).toString();
                                else ccName = pn.getProperty("name").toString();
                                ccName.trim();
                            }
                            if (ccName.length() == 0 && pn.hasProperty("N_Name")) {
                                ccName = Array.get(pn.getProperty("N_Name"), 0).toString().trim();
                            }
                        }
                        if (ccName.equals("\t")) ccName = "";
                        if (ccNode.hasProperty("H_LyChI_L4") && lychi.length() == 0) {
                            lychi = ccNode.getProperty("H_LyChI_L4").toString();
                        }
                    }

                    sb = new StringBuffer();
                    sb.append(ccIDs.toString()+"\t"+ccName.toString()+"\t"+lychi+"\t"+smiles);
                    for (int i=0; i<ccLabels.length; i++) {
                        sb.append("\t"+ccLabels[i]);
                    }
                    sb.append("\n");
                    fos.print(sb.toString());

                } else {
                    System.err.println("orphan node?");
                }
            }
            fos.close();
            tx.success();
        }
    }

}
