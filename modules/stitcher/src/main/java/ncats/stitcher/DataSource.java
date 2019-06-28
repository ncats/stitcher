package ncats.stitcher;

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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;

public class DataSource extends CNode {
    static final Logger logger = Logger.getLogger(DataSource.class.getName());
    public static final String IDFIELD = "IdField";
    public static final String NAMEFIELD = "NameField";
    public static final String STRUCTFIELD = "StructField";
    public static final String EVENTPARSER = "EventParser";

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

    @Override
    public void set (String name, Object value, boolean index) {
        if (KEY.equals(name))
            throw new IllegalArgumentException
                ("Property \""+name+"\" is read-only!");

        super.set(name, value, index);
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

    public String getName () {
        return execute (new Callable<String> () {
                public String call () throws Exception {
                    return (String)_node.getProperty(NAME);
                }
            });
    }

    public Label getLabel () {
        String name = getName ();
        StringBuilder label = new StringBuilder ();
        for (int pos = 0, p; (p = name.indexOf('.', pos)) > 0;) {
            label.append(name.substring(pos, p));
            if (p < name.length() && Character.isDigit(name.charAt(p+1))) {
                // this is a decimal (e.g., version number), so replace with _
                label.append('_');
                pos = p+1;
            }
            else break;
        }
        if (label.length() == 0)
            label.append(name);
        return Label.label("S_"+label.toString().toUpperCase());
    }

    //@Deprecated //!!!! This doesn't add the new name to the correct datasource name index Index<Node> index = gdb.index().forNodes(DataSource.nodeIndexName());
    //public void setName (String name) {
    //    set (NAME, name, true);
    //}

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
