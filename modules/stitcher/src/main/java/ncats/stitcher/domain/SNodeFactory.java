package ncats.stitcher.domain;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

/**
 * Created by katzelda on 2/22/17.
 */
class SNodeFactory {

    public static SNode createFrom(StitcherInstance graphDb, Node neo4jNode){
        return new ConcreteSNode(graphDb, neo4jNode);
    }

    private static class ConcreteSNode extends SNode {
        public ConcreteSNode(StitcherInstance graphDb, Node neo4jNode) {
            super(graphDb, neo4jNode);
        }
    }
}
