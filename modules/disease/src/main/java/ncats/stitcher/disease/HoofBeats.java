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
import static ncats.stitcher.StitchKey.*;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HoofBeats {
    static final Logger logger = Logger.getLogger(HoofBeats.class.getName());

    static class MONDOMapper {
        Map<Entity, Entity> mondo = new HashMap<>();
        Map<Entity, Entity> gard = new HashMap<>();
        final String dsource;
        
        MONDOMapper (EntityFactory ef, String dsource) {
            this.dsource = dsource;
            ef.stitches((source, target, values) -> {
                    map (source, target);
                }, R_exactMatch, R_equivalentClass);
            logger.info("### "+mondo.size()+" MONDO diseases exactly mapped to "
                        +gard.size()+" GARD!");
        }

        void map (Entity source, Entity target) {
            /*
             * first find exact mapping MONDO -> GARD
             */
            boolean next = true;
            if (source.is("S_MONDO") && target.is("S_GARD")) {
            }
            else if (source.is("S_GARD") && target.is("S_MONDO")) {
                Entity t = source;
                source = target;
                target = t;
            }
            else {
                next = false;
            }
            
            if (next) {
                Entity old = mondo.put(source, target);
                if (old != null) {
                    logger.warning("** MONDO "+source.payload("notation")
                                   +" is mapped to GARD "+old.payload("id")
                                   +" and "+target.payload("id"));
                }
                
                old = gard.put(target, source);
                if (old != null) {
                    logger.warning("** GARD "+target.payload("id")+" is mapped "
                                   +"to MONDO "+old.payload("notation")
                                   +" and "+source.payload("notation"));
                }
                next = false;
            }
            else {
                /*
                 * find exact mapping MONDO -> dsource but for those MONDO 
                 * diseases not already mapped exactly to GARD
                 */
                next = true;
                if (source.is("S_MONDO") && target.is(dsource)) {
                }
                else if (source.is(dsource) && target.is("S_MONDO")) {
                    Entity t = source;
                    source = target;
                    target = t;
                }
                else {
                    next = false;
                }
            }

            if (next) {
                // now see if target is connected to a GARD node
                
            }
        } // map()
    }

    final EntityFactory ef;

    public HoofBeats (EntityFactory ef) {
        this.ef = ef;
    }

    public void beats () {
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

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+HoofBeats.class.getName()+" DBDIR");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            HoofBeats hb = new HoofBeats (ef);
            hb.beats();
        }
    }
}
