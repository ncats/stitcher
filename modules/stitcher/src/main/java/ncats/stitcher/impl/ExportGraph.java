package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;
import java.util.concurrent.atomic.AtomicInteger;
import java.text.SimpleDateFormat;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class ExportGraph {
    static final Logger logger = Logger.getLogger(ExportGraph.class.getName());
    
    final EntityFactory ef;
    final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    
    public ExportGraph (EntityFactory ef) {
        this.ef = ef;
    }

    protected void nodeAttrs (PrintStream ps) {
        ps.println
            ("    <attribute id=\"0\" title=\"name\" type=\"string\"/>");
    }

    protected void edgeAttrs (PrintStream ps) {
        ps.println
            ("    <attribute id=\"0\" title=\"type\" type=\"string\"/>");
    }
    
    protected void nodes (PrintStream ps) {
    }

    protected void edges (PrintStream ps) {
    }

    protected void edge (PrintStream ps, long id, Entity source, Entity target,
                         boolean rev, String type) {
        ps.print("    <edge id=\""+id+"\"");
        if (rev)
            ps.print(" source=\""+target.getId()
                     +"\" target=\""+source.getId()+"\"");
        else
            ps.print(" source=\""+source.getId()
                     +"\" target=\""+target.getId()+"\"");
        ps.println(">");
        ps.println("      <attvalues>");
        ps.println("        <attvalue for=\"0\" value=\""+type+"\"/>");
        ps.println("      </attvalues>");
        ps.println("    </edge>");
    }
    
    public void export (OutputStream os) throws Exception {
        final PrintStream ps = new PrintStream (os);
        ps.println("<?xml version=\"1.0\" encoding=\"UTF-8\"?>");
        ps.println("<gexf xmlns=\"http://www.gexf.net/1.2draft\"");
        ps.println("  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema−instance\"");
        ps.println("  xsi:schemaLocation=\"http://www.gexf.net/1.2draft "
                   +"http://www.gexf.net/1.2draft/gexf.xsd\"");
        ps.println("  version=\"1.2\">");
        ps.println(" <meta lastmodifieddate=\""+sdf.format(new Date())+"\">"); 
        ps.println("   <creator>"+getClass().getName()+"</creator>");
        ps.println("   <description>"+ef.getGraphDb().getPath()
                   +"</description>");
        ps.println(" </meta>");
        ps.println(" <graph defaultedegetype=\"directed\">");
        ps.println("   <attributes class=\"node\">");
        nodeAttrs (ps);
        ps.println("   </attributes>");
        ps.println("   <attributes class=\"edge\">");
        edgeAttrs (ps);
        ps.println("   </attributes>");
        ps.println("   <nodes>");
        nodes (ps);
        ps.println("   </nodes>");
        ps.println("   <edges>");
        edges (ps);
        ps.println("   </edges>");
        ps.println(" </graph>");
        ps.println("</gexf>");
    }

    static String escape (Object obj) {
        if (obj != null) {
            String s;
            if (obj.getClass().isArray()) {
                s = ((String[])obj)[0];
            }
            else
                s = (String)obj;
            return s.replaceAll(">", "&gt;").replaceAll("<", "&lt;")
                .replaceAll("\"", "&quot;");
        }
        return "";
    }
    
    public static class ExportGraphGARD_HP extends ExportGraph {
        public ExportGraphGARD_HP (EntityFactory ef) {
            super (ef);
        }

        static boolean checkGARD (Entity e) {
            final Set<Long> nb = new HashSet<>();
            e.neighbors((id, other, key, rev, props) -> {
                    if (key == R_subClassOf
                        || "has_phenotype".equals(props.get("name"))) {
                        nb.add(other.getId());
                        return false;
                    }
                    return true;
                }, R_subClassOf, R_rel);
            return !nb.isEmpty();
        }

        static boolean checkHP (Entity e) {
            final Set<Long> nb = new HashSet<>();
            e.neighbors((id, other, key, rev, props) -> {
                    if ((key == R_subClassOf && other.is("S_HP"))
                        || ("has_phenotype".equals(props.get("name"))
                            && other.is("S_GARD"))) {
                        nb.add(other.getId());
                        return false;
                    }
                    return true;
                }, R_subClassOf, R_rel);
            return !nb.isEmpty();
        }
        
        @Override
        protected void nodes (PrintStream ps) {
            AtomicInteger count = new AtomicInteger ();
            ef.entities("S_GARD", e -> {
                    if (checkGARD (e)) {
                        Map<String, Object> data = e.payload();
                        ps.println("     <node id=\""+e.getId()+"\" label=\""
                                   +data.get("gard_id")+"\">");
                        ps.println("       <attvalues>");
                        ps.println("         <attvalue for=\"0\" value=\""
                                   +escape(data.get("name"))+"\"/>");
                        ps.println("       </attvalues>");
                        ps.println("     </node>");
                        count.getAndIncrement();
                    }
                });
            
            ef.entities("S_HP", e -> {
                    if (checkHP (e)) {
                        Map<String, Object> data = e.payload();
                        String notation = (String)data.get("notation");
                        if (notation != null) {
                            ps.println
                                ("     <node id=\""+e.getId()+"\" label=\""
                                 +notation+"\">");
                            ps.println("       <attvalues>");
                            ps.println("         <attvalue for=\"0\" value=\""
                                       +escape(data.get("label"))+"\"/>");
                            ps.println("       </attvalues>");
                            ps.println("     </node>");
                            count.getAndIncrement();
                        }
                    }
                });
            logger.info("#### "+count+" nodes!");
        }


        void edges (PrintStream ps, Set<Long> edges, Entity e) {
            Set<Long> seen = new HashSet<>();
            e.neighbors((id, other, key, reverse, props) -> {
                    if (edges.contains(id)) {
                    }
                    else {
                        if (!seen.contains(other.getId())) {
                            String type = null;
                            if (key == R_rel && other.is("S_HP")
                                && "has_phenotype".equals(props.get("name"))) {
                                type = (String) props.get("name");
                            }
                            else if (key == R_subClassOf
                                     && other.is("S_GARD")) {
                                type = "isa";
                            }
                            
                            if (type != null) {
                                edge (ps, id, e, other, reverse, type);
                                seen.add(other.getId());
                            }
                        }
                        edges.add(id);
                    }
                    return true;
                }, R_subClassOf, R_rel);
        }
        
        @Override
        protected void edges (PrintStream ps) {
            Set<Long> edges = new HashSet<>();
            ef.entities("S_GARD", e -> {
                    edges (ps, edges, e);
                });
            ef.entities("S_HP", e -> {
                    e.neighbors((id, other, key, rev, props) -> {
                            if (edges.contains(id)) {
                            }
                            else if (other.is("S_HP")) {
                                edge (ps, id, e, other, rev, "isa");
                                edges.add(id);
                            }
                            return true;
                        }, R_subClassOf);
                });
            logger.info("#### "+edges.size()+" edges!");
        }
    }

    public static class ExportGraphOrphanet_HP extends ExportGraph {
        public ExportGraphOrphanet_HP (EntityFactory ef) {
            super (ef);
        }

        static boolean checkORDO (Entity e) {
            // only consider node with subClassOf and hasPhenotype
            final Set<Long> nb = new HashSet<>();
            e.neighbors((id, other, key, rev, props) -> {
                    if (key == R_subClassOf
                        || props.containsKey("frequency")) {
                        nb.add(other.getId());
                        return false;
                    }
                    return true;
                }, R_subClassOf, R_hasPhenotype);
            return !nb.isEmpty();
        }

        static boolean checkHP (Entity e) {
            final Set<Long> nb = new HashSet<>();
            e.neighbors((id, other, key, rev, props) -> {
                    if ((key == R_subClassOf && other.is("S_HP"))
                        || (props.containsKey("frequency")
                            && other.is("S_ORDO_ORPHANET"))) {
                        nb.add(other.getId());
                        return false;
                    }
                    return true;
                }, R_subClassOf, R_hasPhenotype);
            return !nb.isEmpty();
        }
        
        
        @Override
        protected void nodes (PrintStream ps) {
            AtomicInteger count = new AtomicInteger ();
            ef.entities("S_ORDO_ORPHANET", e -> {
                    Map<String, Object> data = e.payload();
                    String id = (String)data.get("notation");
                    if (!e.is("TRANSIENT")
                        && id.startsWith("ORPHA:") && checkORDO (e)) {
                        ps.println("     <node id=\""+e.getId()+"\" label=\""
                                   +data.get("notation")+"\">");
                        ps.println("       <attvalues>");
                        ps.println("         <attvalue for=\"0\" value=\""
                                   +escape(data.get("label"))+"\"/>");
                        ps.println("       </attvalues>");
                        ps.println("     </node>");
                        count.getAndIncrement();
                    }
                });
            
            ef.entities("S_HP", e -> {
                    if (checkHP (e)) {
                        Map<String, Object> data = e.payload();
                        String notation = (String)data.get("notation");
                        if (notation != null) {
                            ps.println
                                ("     <node id=\""+e.getId()+"\" label=\""
                                 +notation+"\">");
                            ps.println("       <attvalues>");
                            ps.println("         <attvalue for=\"0\" value=\""
                                       +escape(data.get("label"))+"\"/>");
                            ps.println("       </attvalues>");
                            ps.println("     </node>");
                            count.getAndIncrement();
                        }
                    }
                });
            logger.info("#### "+count+" nodes!");
        }

        void edges (PrintStream ps, Set<Long> edges, Entity e) {
            Set<Long> seen = new HashSet<>();
            e.neighbors((id, other, key, reverse, props) -> {
                    if (edges.contains(id)) {
                    }
                    else {
                        if (!seen.contains(other.getId())) {
                            boolean check = 
                                (key == R_hasPhenotype
                                 && other.is("S_HP") && checkHP(other));
                            if (!check && key == R_subClassOf
                                && other.is("S_ORDO_ORPHANET")
                                && !other.is("TRANSIENT")) {
                                String orpha = (String)other.payload("notation");
                                check = orpha.startsWith("ORPHA:");
                            }

                            if (check) {
                                String type = key.toString();
                                edge (ps, id, e, other, reverse, type);
                                seen.add(other.getId());
                            }
                        }
                        edges.add(id);
                    }
                    return true;
                }, R_subClassOf, R_hasPhenotype);
        }
        
        @Override
        protected void edges (PrintStream ps) {
            Set<Long> edges = new HashSet<>();
            ef.entities("S_ORDO_ORPHANET", e -> {
                    Map<String, Object> data = e.payload();
                    String id = (String)data.get("notation");
                    if (!e.is("TRANSIENT")
                        && id.startsWith("ORPHA:") && checkORDO (e)) {
                        edges (ps, edges, e);
                    }
                });
            ef.entities("S_HP", e -> {
                    if (checkHP (e)) {
                        e.neighbors((id, other, key, rev, props) -> {
                                if (edges.contains(id)) {
                                }
                                else if (other.is("S_HP")) {
                                    edge (ps, id, e, other, rev, key.toString());
                                    edges.add(id);
                                }
                                return true;
                            }, R_subClassOf);
                    }
                });
            logger.info("#### "+edges.size()+" edges!");
        }        
    } // ExportGraphOrphanet_HP

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+ExportGraph.class.getName()
                        +" DBDIR [OUTFILE]");
            System.exit(1);
        }

        try (EntityFactory ef = new EntityFactory (argv[0])) {
            ExportGraph eg = new ExportGraphOrphanet_HP (ef);
            OutputStream os = System.out;
            if (argv.length > 1) {
                os = new FileOutputStream (argv[1]);
            }
            eg.export(os);
            if (os != System.out)
                os.close();
        }
    }
}
