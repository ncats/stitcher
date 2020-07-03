package ncats.stitcher.impl;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class OntLookup extends EntityFactory {
    static final Logger logger = Logger.getLogger(OntLookup.class.getName());

    public OntLookup (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public OntLookup (String dir) throws IOException {
        super (dir);
    }
    
    public OntLookup (File dir) throws IOException {
        super (dir);
    }

    void lookup (String id, Function<Map<String, Object>, Boolean> f)
        throws Exception {
        for (Iterator<Entity> iter = find ("notation", id); iter.hasNext();) {
            Entity e = iter.next();
            if (!f.apply(e.payload()))
                break;
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: " +OntLookup.class.getName()
                        +" DBDIR FILES...");
            System.exit(1);
        }
        
        try (OntLookup lut = new OntLookup (argv[0])) {
            for (int i = 1; i < argv.length; ++i) {
                File file = new File (argv[i]);
                BufferedReader br = new BufferedReader (new FileReader (file));
                for (String line; (line = br.readLine()) != null; ) {
                    String id = line.trim();
                    lut.lookup("ORPHA:"+id, d -> {
                            System.out.println(id+"\t"+d.get("label"));
                            return false;
                        });
                }
                br.close();
            }
        }
    }
}
