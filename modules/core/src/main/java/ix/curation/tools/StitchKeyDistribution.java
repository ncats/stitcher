package ix.curation.tools;

import java.util.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.EntityFactory;

public class StitchKeyDistribution {
    static final Logger logger = Logger.getLogger
        (StitchKeyDistribution.class.getName());
    
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: "+StitchKeyDistribution.class.getName()
                               +" DB [KEYS/LABELS...] [MAX=10]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            EnumSet<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
            int max = 10;

            Set<String> labels = new TreeSet<String>();
            for (int i = 1; i < argv.length; ++i) {
                try {
                    keys.add(StitchKey.valueOf(argv[i]));
                }
                catch (Exception ex) {
                    try {
                        max = Integer.parseInt(argv[i]);
                    }
                    catch (NumberFormatException e) {
                        /*
                        logger.log(Level.SEVERE, "Argument \""+argv[i]+"\" is"
                                   +" neither a stitchkey nor integer!");
                        */
                        // assume this is label
                        labels.add(argv[i]);
                    }
                }
            }
            
            if (keys.isEmpty()) {
                keys = EnumSet.allOf(StitchKey.class);
            }

            if (!labels.isEmpty()) {
                System.out.println("### Node labels: "+labels);
            }
            
            EntityFactory ef = new EntityFactory (graphDb);
            for (StitchKey key : keys) {
                final Map<Object, Integer> dist =
                    ef.getStitchedValueDistribution
                    (key, labels.toArray(new String[0]));
                
                if (!dist.isEmpty()) {
                    System.out.println("++++++++++ "+key+" +++++++++++");
                    Set sorted = new TreeSet (new Comparator () {
                            public int compare (Object o1, Object o2) {
                                return dist.get(o2) - dist.get(o1);
                            }
                        });
                    sorted.addAll(dist.keySet());
                    int c = 0;
                    for (Object k : sorted) {
                        System.out.println(k+": "+dist.get(k));
                        if (++c > max)
                            break;
                    }
                }
            }
        }
        finally {
            graphDb.shutdown();
        }
    }
}
