package ncats.stitcher.domain;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Created by katzelda on 2/22/17.
 */
public class StitcherInstance implements Closeable{
    private static final Label COMPONENT_LABEL = Label.label("COMPONENT");

    private final  GraphDatabaseService graphDb;

    private final ConcurrentHashMap<String, SNodeLabel> labels = new ConcurrentHashMap<>();


    private StitcherInstance(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public static StitcherInstance createFrom(File neo4jDir) throws IOException{
        return new StitcherInstance(new GraphDatabaseFactory().newEmbeddedDatabase(neo4jDir));
    }

    public Stream<Component> components(){
        return TransactionStream.create(graphDb.beginTx(),
                () -> graphDb.findNodes(COMPONENT_LABEL).stream().map(n -> new Component(this, n)));
    }

    public SNodeLabel getLabel(String labelName){
        return labels.computeIfAbsent(labelName, n -> new SNodeLabel(n));
    }

    public GraphDatabaseService getGraphDb() {
        return graphDb;
    }


    @Override
    public void close() throws IOException {
        graphDb.shutdown();
    }
}
