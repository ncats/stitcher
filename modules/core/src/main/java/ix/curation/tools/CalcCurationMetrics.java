package ix.curation.tools;


import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ix.curation.GraphDb;
import ix.curation.Util;
import ix.curation.EntityFactory;
import ix.curation.CurationMetrics;

public class CalcCurationMetrics {
    static final Logger logger = Logger.getLogger
        (CalcCurationMetrics.class.getName());
    
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: "+CalcCurationMetrics.class.getName()
                               +" DB [TAG]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EntityFactory ef = new EntityFactory (graphDb);
            CurationMetrics metrics;
            if (argv.length == 1) {
                logger.info("Calculating graph metrics...");
                metrics = ef.calcCurationMetrics();
            }
            else {
                logger.info("Calculating graph metrics for "+argv[1]+"...");
                metrics = ef.calcCurationMetrics(argv[1]);
            }
            System.out.println(Util.toJson(metrics));
        }
        finally {
            graphDb.shutdown();
        }
    }
}
