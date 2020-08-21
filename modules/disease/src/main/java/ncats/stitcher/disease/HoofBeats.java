package ncats.stitcher.disease;


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

public class HoofBeats {
    static final Logger logger = Logger.getLogger(HoofBeats.class.getName());

    final EntityFactory ef;

    public HoofBeats (EntityFactory ef) {
        this.ef = ef;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+HoofBeats.class.getName()+" DBDIR");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            HoofBeats hb = new HoofBeats (ef);
        }
    }
}
