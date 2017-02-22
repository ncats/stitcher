package ncats.stitcher.domain;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by katzelda on 2/22/17.
 */
public class Component extends SNode{

    private static final RelationshipType PAYLOAD_TYPE = RelationshipType.withName("PAYLOAD");

    Component(StitcherInstance graphDb, Node node) {
       super(graphDb, node);
    }

    public Stream<SNode> payloads(){
        return payloads(false);
    }
    public Stream<SNode> payloads(boolean parallel){
        Node n = getWrappedNode();
        GraphDatabaseService graphDb = getGraphDb();

        return TransactionStream.create(getGraphDb().beginTx(), () -> StreamSupport.stream(getWrappedNode().getRelationships(PAYLOAD_TYPE)
                                                                .spliterator(), parallel)
                                                                .map( r -> SNodeFactory.createFrom(getStitcherInstance(), r.getOtherNode(n)) ));
    }

}
