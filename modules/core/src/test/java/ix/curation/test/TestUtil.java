package ix.curation.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.traversal.*;
import org.neo4j.tooling.GlobalGraphOperations;

import ix.curation.*;

public class TestUtil {
    static final Logger logger = Logger.getLogger(TestUtil.class.getName());

    public static GraphDatabaseService createTempDb () {
        return createTempDb ("");
    }

    public static File createTempDir () throws IOException {
        return createTempDir (null);
    }
    
    public static File createTempDir (String name) throws IOException {
        File file = File.createTempFile("_ix"+(name != null ? name:""),
                                        ".db", new File ("."));
        file.delete();
        file.mkdirs();
        return file;
    }
    
    public static GraphDatabaseService createTempDb (String name) {
        try {
            return Util.openGraphDb(createTempDir (name));
        }
        catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        return null;
    }
}
