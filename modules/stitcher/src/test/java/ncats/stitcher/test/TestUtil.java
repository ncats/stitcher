package ncats.stitcher.test;

import java.util.*;
import java.util.concurrent.Callable;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.Logger;
import java.util.logging.Level;

import org.neo4j.graphdb.GraphDatabaseService;
import ncats.stitcher.*;

public class TestUtil {
    static final Logger logger = Logger.getLogger(TestUtil.class.getName());

    public static GraphDatabaseService createTempDb () {
        return createTempDb ("_ix");
    }

    public static File createTempDir () throws IOException {
        return createTempDir ("_ix");
    }
    
    public static File createTempDir (String name) throws IOException {
        Path path = Files.createTempDirectory(name);
        return path.toFile();
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
