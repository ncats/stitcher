package ix.curation.impl;

import ix.curation.*;
import org.apache.log4j.Logger;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.EnumMap;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;

public class ChemblTargetEntityFactory extends EntityRegistry<Target> {
    static Logger log;

    protected EnumMap<StitchKey, Set<String>> stitches =
            new EnumMap<StitchKey, Set<String>>(StitchKey.class);

    public ChemblTargetEntityFactory(GraphDatabaseService gdb) {
        super(gdb);
    }

    public void clear() {
        stitches.clear();
    }

    public void add(StitchKey key, String property) {
        Set<String> props = stitches.get(key);
        if (props == null) {
            stitches.put(key, props = new TreeSet<String>());
        }
        props.add(property);
    }

    public Set<String> get(StitchKey key) {
        return stitches.get(key);
    }

    @Override
    public Entity register (final Target target) {
        // execute in transaction context
        return execute (new Callable<Entity> () {
                public Entity call () throws Exception {
                    return _register (target);
                }
            });
    }

    public Entity _register (Target target) {
        Entity ent = Entity._getEntity(_createNode(EntityType.TargetBiology));
        DefaultPayload payload = new DefaultPayload(getDataSource());
        ent._node().setProperty("TargetType", target.type.toString());
        ent._node().setProperty(StitchKey.N_Name.toString(), target.name);
        ent._node().setProperty(StitchKey.I_ChEMBL.toString(), target.id);
        if (target.acc != null) ent._node().setProperty(StitchKey.I_UniProt.toString(), target.acc);
        payload.setId(target.id);
        ent._add(payload);
        return ent;
    }

    public void addChemblTargetClasses(String[] classes) {
        // i+1'th element is childOf i'th element
        for (int i = 0; i < classes.length; i++) {
            if (classes[i] == null) continue;
            Target t = new Target();
            t.id = classes[i];
            t.name = classes[i];
            t.type = TargetType.TargetClass;
            Entity e = register(t);//, StitchKey.I_ChEMBL.toString(), t.id);
            Node node = e._node();

            if (i > 0) { // make link to parent (ie preceding class)
                Index<Node> index = gdb.index().forNodes("node." + StitchKey.I_ChEMBL.toString());
                try (IndexHits<Node> hits = index.get(StitchKey.I_ChEMBL.toString(), classes[i - 1])) {
                    for (Node n : hits) {
                        if (n.equals(node)) continue;
                        // get relationships for this hit, ignore any contain the current node
                        Iterable<Relationship> foo = n.getRelationships(PredicateType.ChildOf, Direction.INCOMING);
                        boolean edgeExists = false;
                        for (Relationship r : foo) {
                            if (r.getOtherNode(n).equals(node)) {
                                edgeExists = true;
                                break;
                            }
                        }
                        if (edgeExists) continue;
                        node.createRelationshipTo(n, PredicateType.ChildOf);
                    }
                }
            }
        }
    }

    /**
     * Check wether an edge exists between two nodes.
     *
     * @param src    Start node
     * @param target End node
     * @param type   The edge type (an enum from @link{PredicateType}
     * @return <code>true</code> if the edge is present, <code>false</code> otherwise
     */
    private boolean relationExists(Node src, Node target, PredicateType type) {
        Iterable<Relationship> rels = src.getRelationships(type, Direction.INCOMING);
        for (Relationship r : rels) {
            if (r.getOtherNode(src).equals(target))
                return true;
        }
        return false;
    }

    public void addChemblTarget(Target t, String targetClass) {
        Entity e;

        if (t.acc != null)
            e = register(t);//, StitchKey.I_UniProt.toString(), t.acc);
        else // non SINGLE PROTEIN targets will not have access, so index on id
            e = register(t);//, StitchKey.I_ChEMBL.toString(), t.id);

        Node node = e._node();

        // target classes nodes are indexed on StitchKey.I_ChEMBL since they don't have a UniProt accession
        Index<Node> index = gdb.index().forNodes("node." + StitchKey.I_ChEMBL.toString());
        try (IndexHits<Node> hits = index.get(StitchKey.I_ChEMBL.toString(), targetClass)) {
            for (Node n : hits) {
                if (!relationExists(n, node, PredicateType.IsA))
                    node.createRelationshipTo(n, PredicateType.IsA);
            }
        }

        // create an index for the uniprot id, so we can link to targets
        // via index queries on uniprot id in other classes
        index = gdb.index().forNodes("node." + StitchKey.I_UniProt.toString());
        if (t.acc != null) index.add(node, StitchKey.I_UniProt.toString(), t.acc);
        index = gdb.index().forNodes("node." + StitchKey.I_ChEMBL.toString());
        index.add(node, StitchKey.I_ChEMBL.toString(), t.id);
    }


    public static void main(String[] argv) throws Exception {
        log = Logger.getLogger(ChemblTargetEntityFactory.class);

        if (argv.length < 1) {
            System.err.println("Usage: " + ChemblTargetEntityFactory.class.getName() + " DBDIR ");
            System.exit(1);
        }

        Connection conn = DriverManager.getConnection("jdbc:mysql://volta.ncats.nih.gov/chembl_20?user=chembl_20&password=chembl_20");
        PreparedStatement pst = conn.prepareStatement("SELECT  " +
                "    distinct td.tid, td.chembl_id, td.pref_name, description, td.target_type, accession, pfc . * " +
                "FROM " +
                "    target_dictionary td, " +
                "    target_components tc, " +
                "    component_sequences cs, " +
                "    component_class cc, " +
                "    protein_family_classification pfc " +
                "WHERE " +
                "td.tax_id = 9606 AND  " +
                "td.tid = tc.tid " +
                "        AND tc.component_id = cs.component_id " +
                "        AND cc.component_id = cs.component_id " +
                "        AND pfc.protein_class_id = cc.protein_class_id");
        ResultSet rset = pst.executeQuery();

        GraphDatabaseService gdb = new GraphDatabaseFactory()
                .newEmbeddedDatabase(argv[0]);

        try (Transaction tx = gdb.beginTx()) {
            ChemblTargetEntityFactory ctef = new ChemblTargetEntityFactory(gdb);
            while (rset.next()) {
                String[] tclasses = new String[8];
                for (int i = 1; i <= 8; i++) {
                    tclasses[i - 1] = rset.getString("l" + i);
                }
                ctef.addChemblTargetClasses(tclasses);

                // Add in the actual target, and link to most specific target class
                String targetClass = null;
                for (int i = 1; i <= 8; i++) {
                    if (tclasses[i] == null) {
                        targetClass = tclasses[i - 1];
                        break;
                    }
                }
                Target t = new Target();
                t.id = rset.getString("chembl_id");
                t.acc = rset.getString("accession");
                t.name = rset.getString("pref_name");
                String tt = rset.getString("target_type");
                if (tt.equals("SINGLE PROTEIN"))
                    t.type = TargetType.MolecularTarget;
                else {
                    t.type = TargetType.TargetFamily;
                    t.acc = null;
                }
                t.taxId = 9606;

                ctef.addChemblTarget(t, targetClass);
            }
            tx.success();
        } finally {
            gdb.shutdown();

            pst.close();
            rset.close();
            conn.close();
        }
    }
}
