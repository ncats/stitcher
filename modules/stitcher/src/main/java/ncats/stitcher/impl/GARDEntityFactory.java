package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.security.MessageDigest;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * sbt stitcher/"runMain ncats.stitcher.impl.GARDEntityFactory DBDIR JDBC"
 * where JDBC is in the format
 *   jdbc:mysql://garddb-dev.ncats.io/gard?user=XXX&password=ZZZZ
 */
public class GARDEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(GARDEntityFactory.class.getName());

    public GARDEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public GARDEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public GARDEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public int register (Connection con) throws Exception {
        int count = 0;
        Statement stm = con.createStatement();
        try (ResultSet rset = stm.executeQuery
             ("select * from GARD_AllDiseases")) {
            while (rset.next()) {
                long id = rset.getLong("DiseaseID");
                String name = rset.getString("DiseaseName");
                String gid = String.format("GARD:%1$05d", id);
                System.out.println(gid+"\t"+name);
            }
        }
        
        return count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+GARDEntityFactory.class.getName()
                        +" DBDIR JDBC_URL");
            System.exit(1);
        }

        try (GARDEntityFactory gef = new GARDEntityFactory (argv[0]);
             Connection con = DriverManager.getConnection(argv[1])) {
            gef.register(con);
        }
    }
}
