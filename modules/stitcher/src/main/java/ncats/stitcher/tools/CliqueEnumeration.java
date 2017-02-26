package ncats.stitcher.tools;

import java.util.*;
import java.io.*;
import java.util.logging.Logger;
import java.util.logging.Level;

import ncats.stitcher.EntityFactory;
import ncats.stitcher.Entity;
import ncats.stitcher.GraphDb;
import ncats.stitcher.CliqueVisitor;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;
import ncats.stitcher.Clique;

public class CliqueEnumeration implements CliqueVisitor {
    static final Logger logger =
        Logger.getLogger(CliqueEnumeration.class.getName());

    List<Clique> cliques = new ArrayList<Clique>();
    Map<Long, BitSet> nodes = new TreeMap<Long, BitSet>();

    final EntityFactory ef;
    public CliqueEnumeration (GraphDb graphDb) {
        ef = new EntityFactory (graphDb);
    }

    public void enumerate (StitchKey[] keys, String label) {
        ef.cliques(label, this, keys);

        List<Clique> sorted = new ArrayList<Clique>(cliques);
        Collections.sort(sorted, new Comparator<Clique>() {
                public int compare (Clique c1, Clique c2) {
                    long d = rank (c2) - rank (c1);
                    if (d == 0l) {
                        d = c2.size() - c1.size();
                    }
                    if (d == 0l) return 0;
                    return d < 0l ? -1 : 1;
                }
            });

        System.out.println("### "+cliques.size()+" cliques!");
        for (Clique clique : sorted) {
            System.out.println
                ("+++++++ " +String.format("%1$5d.", cliques.indexOf(clique))
                 +" Clique ("+clique.size()+") ++++++++");
            System.out.println("## rank: "+rank (clique));
            System.out.print("## nodes: [");
            for (Entity e : clique.entities())
                System.out.print(" "+e.getId()+",");
            System.out.println("]");
            System.out.print("## values: ");
            for (Map.Entry<StitchKey, Object> me : clique.values().entrySet())
                System.out.print
                    (" "+me.getKey()+"="+Util.toString(me.getValue()));
            System.out.println();
        
            for (Entity e : clique.entities())
                System.out.println
                    (String.format("%1$10d", e.getId())+" "+e.datasource());
        }

        System.out.println();
        System.out.println("### "+nodes.size()+" nodes!");
        for (Map.Entry<Long, BitSet> me : nodes.entrySet()) {
            BitSet bs = me.getValue();
            if (bs.cardinality() > 1) {
                System.out.println
                    ("Node "+me.getKey()+" is member of cliques "+bs);
            }
        }
    }

    static long rank (Clique clique) {
        long rank = 0l;
        for (StitchKey key : clique.values().keySet()) {
            rank |= 1l << key.ordinal();
        }
        return rank;
    }
    
    /**
     * CliqueVisitor interface
     */
    public boolean clique (Clique clique) {
        int index = cliques.size();
        for (Entity e : clique.entities()) {
            BitSet c = nodes.get(e.getId());
            if (c == null) {
                nodes.put(e.getId(), c = new BitSet ());
            }
            c.set(index);
        }
        cliques.add(clique);
        
        return true;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            System.err.println("Usage: "+CliqueEnumeration.class.getName()
                               +" DB [StitchKeys...]");
            System.exit(1);
        }

        GraphDb graphDb = GraphDb.getInstance(argv[0]);
        try {
            CliqueEnumeration clique = new CliqueEnumeration (graphDb);
            
            EnumSet<StitchKey> keys = EnumSet.noneOf(StitchKey.class);
            String label = null;
            for (int i = 1; i < argv.length; ++i) {
                try {
                    StitchKey key = StitchKey.valueOf(argv[i]);
                    keys.add(key);
                }
                catch (Exception ex) {
                    /*
                    logger.warning("** Ignore bogus StitchKey \""
                                   +argv[i]+"\"!");
                    */
                    // assume node label
                    label = argv[i];
                }
            }

            clique.enumerate(keys.toArray(new StitchKey[0]), label);
        }
        finally {
            graphDb.shutdown();
        }
    }
}
