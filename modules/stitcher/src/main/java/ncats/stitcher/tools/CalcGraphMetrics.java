package ncats.stitcher.tools;


import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.GraphDb;
import ncats.stitcher.Util;
import ncats.stitcher.EntityFactory;
import ncats.stitcher.GraphMetrics;

public class CalcGraphMetrics {
    static final Logger logger = Logger.getLogger
        (CalcGraphMetrics.class.getName());
    
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: "+CalcGraphMetrics.class.getName()
                               +" DB [TAG]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            GraphMetrics metrics;
            if (argv.length == 1) {
                logger.info("Calculating graph metrics...");
                metrics = ef.calcGraphMetrics();
            }
            else {
                logger.info("Calculating graph metrics for "+argv[1]+"...");
                metrics = ef.calcGraphMetrics(argv[1]);
            }
            System.out.println(Util.toJson(metrics));
        }
        finally {
            graphDb.shutdown();
        }
    }
}
