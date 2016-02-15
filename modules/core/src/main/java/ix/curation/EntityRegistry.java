package ix.curation;

import java.net.URL;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;

public abstract class EntityRegistry<T> extends EntityFactory {
    static final Logger logger =
        Logger.getLogger(EntityRegistry.class.getName());

    protected DataSource source;
    protected DataSourceFactory dsf;
    
    public EntityRegistry (String dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }
    public EntityRegistry (File dir) throws IOException {
        this (GraphDb.getInstance(dir));
    }
    public EntityRegistry (GraphDatabaseService gdb) {
        this (GraphDb.getInstance(gdb));
    }
    
    public EntityRegistry (GraphDb graphDb) {
        super (graphDb);
        dsf = new DataSourceFactory (graphDb);
        init ();
    }

    // to be overriden by subclass
    protected void init () {
    }

    protected Node _createNode (EntityType type) {
        if (source == null) {
            throw new IllegalStateException
                ("Can't create entity without a data source!");
        }
        
        Node node = gdb.createNode(type, AuxNodeType.ENTITY,
                                   DynamicLabel.label(source.getKey()));
        node.setProperty(SOURCE, source.getKey());
        return node;
    }
    
    protected Node createNode (EntityType type) {
        try (Transaction tx = gdb.beginTx()) {
            Node node = _createNode (type);
            tx.success();
            return node;
        }
    }
    
    public DataSourceFactory getDataSourceFactory () { return dsf; }
    public void setDataSource (DataSource source) {
        this.source = source;
    }
    public DataSource getDataSource () { return source; }

    public DataSource register (File file) throws IOException {
        return source = getDataSourceFactory().register(file);
    }

    public DataSource register (URL url) throws IOException {
        return source = getDataSourceFactory().register(url);
    }

    public abstract Entity register (T data);
}
