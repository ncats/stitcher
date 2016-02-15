package ix.curation;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipException;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.Callable;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

public class DataSource extends CNode {
    static final Logger logger = Logger.getLogger(DataSource.class.getName());

    public static String nodeIndexName () {
        return DataSource.class.getName()+NODE_INDEX;
    }

    public static String relationshipIndexName () {
        return DataSource.class.getName()+RELATIONSHIP_INDEX;
    }
    
    public static DataSource getDataSource (Node node) {
        try (Transaction tx = node.getGraphDatabase().beginTx()) {
            DataSource source = new DataSource (node);
            tx.success();
            return source;
        }
    }
    
    public static DataSource _getDataSource (Node node) {
        return new DataSource (node);
    }

    protected DataSource (Node node) {
        super (node);
    }

    public void set (String name, Object value) {
        set (name, value, false);
    }
    
    public void set (String name, Object value, boolean index) {
        if (KEY.equals(name))
            throw new IllegalArgumentException
                ("Property \""+name+"\" is read-only!");
        
        try (Transaction tx = getGraphDb().beginTx()) {
            if (_node.hasProperty(name)) {
                Object old = _node.getProperty(name);
                if (!value.equals(old)) {
                    _snapshot (name, old, value);
                    if (index) {
                        gdb.index().forNodes
                            (AuxNodeType.class.getName())
                            .add(_node, name, value);
                    }
                }
            }
            else {
                _snapshot (name, null, value);
                if (index) {
                    gdb.index().forNodes
                        (AuxNodeType.class.getName()).add(_node, name, value);
                }
            }
            tx.success();
        }
    }

    public URI toURI () {
        try (Transaction tx = getGraphDb().beginTx()) {
            URI uri = null;
            if (_node.hasProperty(Props.URI)) {
                try {
                    uri = new URI ((String)_node.getProperty(Props.URI));
                }
                catch (Exception ex) {
                    logger.log(Level.SEVERE, "Bogus URI stored in Node "
                               +_node.getId()+": "
                               +_node.getProperty(Props.URI));
                }
            }
            tx.success();
            return uri;
        }
    }

    InputStream getInputStream () throws IOException {
        URI uri = toURI ();
        InputStream is = null;
        if (uri != null) {
            File file = Util.getLocalFile(uri);
            is = file.exists() ? new FileInputStream (file)
                : toURI().toURL().openStream();
        }
        return is;
    }
    
    public InputStream openStream () throws IOException {
        InputStream is = null;
        try {
            is = getInputStream ();
            if (is != null)
                return new GZIPInputStream (is);
        }
        catch (ZipException ex) {
            // not a zip file
            is = getInputStream ();
        }
        return is;
    }

    public Object get (String name) {
        Object value = null;
        try (Transaction tx = getGraphDb().beginTx()) {
            if (_node.hasProperty(name)) {
                value = _node.getProperty(name);
            }
            tx.success();
            return value;
        }
    }

    public String getName () {
        return execute (new Callable<String> () {
                public String call () throws Exception {
                    return (String)_node.getProperty(NAME);
                }
            });
    }

    public void setName (String name) {
        set (NAME, name, true);
    }

    public String toString () {
        return getName ();
    }

    public String _getKey () {
        return (String)_node.getProperty(KEY);
    }
    
    public String getKey () {
        return execute (new Callable<String> () {
                public String call () throws Exception {
                    return (String)_node.getProperty(KEY);
                }
            });
    }
}
