package ncats.stitcher.domain;

import ncats.stitcher.Props;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by katzelda on 2/22/17.
 */
public abstract class SNode {

    private final Node n;
    private final StitcherInstance instance;

    protected SNode(StitcherInstance instance, Node neo4jNode){
        this.instance = Objects.requireNonNull(instance);

        this.n = Objects.requireNonNull(neo4jNode);
    }

    protected Node getWrappedNode(){
        return n;
    }

    protected GraphDatabaseService getGraphDb(){
        return instance.getGraphDb();
    }

    protected StitcherInstance getStitcherInstance() {
        return instance;
    }

    public List<String> getNames(){
        return Arrays.asList( (String[]) n.getProperty("N_Name"));
    }
    public Stream<SNode> getNeighbors(){
        return getNeighbors(false);
    }
    public Stream<SNode> getNeighbors(boolean parallel){
        Node n = getWrappedNode();
        GraphDatabaseService graphDb = getGraphDb();

        return TransactionStream.create(getGraphDb().beginTx(), () -> StreamSupport.stream(getWrappedNode().getRelationships()
                .spliterator(), parallel)
                .map( r -> SNodeFactory.createFrom(instance, r.getOtherNode(n)) ));
    }


    public boolean hasLabel(String labelName){
        return n.hasLabel(instance.getLabel(labelName).label);
    }

    public Map<String, List<String>> getPropertyMap(){
        Map<String, List<String>> map = new LinkedHashMap<>();
        for(Map.Entry<String, Object> en : n.getAllProperties().entrySet()){
            if(en.getValue().getClass().isArray()){
                map.put(en.getKey(), Arrays.asList((String[]) en.getValue()));
                System.out.println(en.getKey() + " : " + Arrays.toString((String[]) en.getValue()));
            }else{
                map.put(en.getKey(),Collections.singletonList(en.getValue().toString()));
            }
        }

        return map;
    }
}
