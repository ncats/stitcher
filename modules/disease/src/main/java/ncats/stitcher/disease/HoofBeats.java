package ncats.stitcher.disease;

import java.io.*;
import java.util.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.lang.reflect.Array;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import ncats.stitcher.graph.UnionFind;
import static ncats.stitcher.StitchKey.*;

import com.fasterxml.jackson.databind.ObjectMapper;

public class HoofBeats {
    static final Logger logger = Logger.getLogger(HoofBeats.class.getName());

    final EntityFactory ef;
    final UnionFind uf = new UnionFind ();
    
    public HoofBeats (EntityFactory ef) {
        this.ef = ef;
    }

    public void test () {
        ObjectMapper mapper = new ObjectMapper ();
        ef.stitches((source, target, values) -> {
                try {
                    logger.info("["+source.getId()+", "+target.getId()+"] "
                                +mapper.writerWithDefaultPrettyPrinter()
                                .writeValueAsString(values));
                }
                catch (Exception ex) {
                    ex.printStackTrace();
                }
            }, I_CODE, N_Name);
    }

    static String getIds (Component component, String source, String prop) {
        StringBuilder sb = new StringBuilder ();
        Entity[] entities = component.stream()
            .filter(e -> e.is(source)).toArray(Entity[]::new);
        if (entities.length > 0) {
            sb.append(entities[0].payload(prop));
            for (int i = 1; i < entities.length; ++i)
                sb.append(","+entities[i].payload(prop));
        }
        return sb.toString();
    }

    void beats (PrintStream ps, long[] comp) throws IOException {
        Component component = ef.component(comp);
        Map<String, Integer> labels = component.labels();
        Map<Object, String> names = new TreeMap<>();
        Map<Object, String> codes = new TreeMap<>();
        Set<Long> all = new HashSet<>();
        
        Map<Object, Integer> values = component.values(N_Name);
        for (Map.Entry<Object, Integer> me : values.entrySet()) {
            long[] nodes = ef.nodes(N_Name.name(), me.getKey());
            for (int i = 0; i < nodes.length; ++i)
                all.add(nodes[i]);
            names.put(me.getKey(), me.getValue()+"/"+nodes.length);
        }
        values = component.values(I_CODE);
        for (Map.Entry<Object, Integer> me : values.entrySet()) {
            long[] nodes = ef.nodes(I_CODE.name(), me.getKey());
            for (int i = 0; i < nodes.length; ++i)
                all.add(nodes[i]);
            codes.put(me.getKey(), me.getValue()+"/"+nodes.length);
        }
        logger.info("### "+component+" "+labels
                    +" N_Name="+names+" I_CODE="+codes+"\n*** "
                    +ef.component(Util.toArray(all)));
        
        ps.print(getIds (component, "S_MONDO", "notation")+"\t");
        ps.print(getIds (component, "S_ORDO_ORPHANET", "notation")+"\t");
        ps.print(getIds (component, "S_GARD", "gard_id")+"\t");
        ps.print(getIds (component, "S_OMIM", "notation")+"\t");
        ps.print(getIds (component, "S_MESH", "notation")+"\t");
        ps.print(getIds (component, "S_DOID", "notation")+"\t");
        ps.print(getIds (component, "S_MEDLINEPLUS", "notation")+"\t");
        ps.print(getIds (component, "S_EFO", "notation"));
        ps.println();
    }
    
    public void beats (String outfile) throws IOException {
        ef.stitches((source, target, values) -> {
                logger.info(source.getId()+": "+source.labels()
                            +" <=> "+target.getId()+": "
                            +target.labels()+" "+values);
                uf.union(source.getId(), target.getId());
            }, R_exactMatch, R_equivalentClass);

        File file = new File (outfile);
        PrintStream ps = new PrintStream (new FileOutputStream (file));
        ps.println("MONDO\tOrphanet\tGARD\tOMIM\tMeSH\tDOID\tMedLinePlus\tEFO");
        long[][] components = uf.components();
        for (long[] comp : components) {
            beats (ps, comp);
        }
        ps.close();
        logger.info("**** "+components.length+" components!");
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+HoofBeats.class.getName()+" DBDIR");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            HoofBeats hb = new HoofBeats (ef);
            hb.beats("zebra_beats.txt");
        }
    }
}
