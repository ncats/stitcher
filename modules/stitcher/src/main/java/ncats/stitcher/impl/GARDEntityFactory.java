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

    static class GARD implements AutoCloseable {
        String[] questions = {
            "GARD_Cause",
            "GARD_Diagnosis",
            "GARD_Inheritance",
            "GARD_Prognosis",
            "GARD_Summary",
            "GARD_SymptomText",
            "GARD_Treatments"
        };
        Map<String, PreparedStatement> pstms = new HashMap<>();
        PreparedStatement otherNames;
        PreparedStatement categories;
        
        GARD (Connection con) throws SQLException {
            for (String q : questions) {
                PreparedStatement pstm = con.prepareStatement
                    ("select a.* from GARD_Questions a, "+q+" b "
                     +"where a.QuestionID = b.QuestionID "
                     +"and b.DiseaseID = ? and a.isDeleted = 0 "
                     +"and a.isSpanish = 0");
                pstms.put(q, pstm);
            }
            otherNames = con.prepareStatement
                ("select DiseaseIdentifier "
                 +"from GARD_OtherNames where DiseaseID = ?");
            categories = con.prepareStatement
                ("select diseasetypename,source "
                 +"from GARD_Categories where DiseaseID = ?");
        }

        public Map<String, Object> instrument (long id) throws SQLException {
            Map<String, Object> data = new TreeMap<>();
            data.put("gard_id", String.format("GARD:%1$05d", id));
            data.put("id", id);
            for (Map.Entry<String, PreparedStatement> me : pstms.entrySet()) {
                String field = me.getKey().replaceAll("GARD_", "");
                PreparedStatement pstm = me.getValue();
                pstm.setLong(1, id);
                try (ResultSet rset = pstm.executeQuery()) {
                    List<String> texts = new ArrayList<>();
                    while (rset.next()) {
                        String text = rset.getString("Answer");
                        if (text != null)
                            texts.add(text.replaceAll("\"", ""));
                    }
                    
                    if (texts.isEmpty()) {
                    }
                    else if (texts.size() == 1) {
                        data.put(field, texts.get(0));
                    }
                    else data.put(field, texts.toArray(new String[0]));
                }
            }
            
            otherNames.setLong(1, id);
            try (ResultSet rset = otherNames.executeQuery()) {
                List<String> names = new ArrayList<>();
                while (rset.next()) {
                    String s = rset.getString(1);
                    if (s != null)
                        names.add(s.replaceAll("\"", ""));
                }
                
                if (!names.isEmpty())
                    data.put("synonyms", names.toArray(new String[0]));
            }
            
            categories.setLong(1, id);
            try (ResultSet rset = categories.executeQuery()) {
                Set<String> cats = new TreeSet<>();
                Set<String> sources = new TreeSet<>();
                while (rset.next()) {
                    String t = rset.getString("diseasetypename");
                    if (t != null)
                        cats.add(t.replaceAll("\"",""));
                    
                    String s = rset.getString("source");
                    if (s != null && !"null".equalsIgnoreCase(s))
                        sources.add(s);
                }
                
                if (!cats.isEmpty())
                    data.put("categories", cats.toArray(new String[0]));
                
                if (!sources.isEmpty())
                    data.put("sources", sources.toArray(new String[0]));
            }
            return data;
        }

        public void close () throws Exception {
            for (PreparedStatement pstm : pstms.values())
                pstm.close();
            otherNames.close();
            categories.close();
        }
    }

    public GARDEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public GARDEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public GARDEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("gard_id");
        setNameField ("name");
        add (N_Name, "name")
            .add(N_Name, "synonyms")
            .add(I_CODE, "gard_id")
            .add(T_Keyword, "categories")
            .add(T_Keyword, "sources")
            ;
    }
        
    public DataSource register (Connection con) throws Exception {
        return register (con, 1);
    }
    
    public DataSource register (Connection con, int version)
        throws Exception {
        DataSource ds = getDataSourceFactory().register("GARD_v"+version);
        
        Statement stm = con.createStatement();        
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null) {
            try (ResultSet rset = stm.executeQuery
                 ("select count(*) from GARD_AllDiseases")) {
                int cnt = rset.getInt(1);
                if (cnt == size) {
                    logger.info(ds.getName()+" ("+size
                                +") has already been registered!");
                    return ds;
                }
            }
        }
        
        setDataSource (ds);
        try (ResultSet rset = stm.executeQuery
             ("select * from GARD_AllDiseases");
             GARD gard = new GARD (con)) {
            int count = 0;
            for (; rset.next(); ++count) {
                long id = rset.getLong("DiseaseID");
                String name = rset.getString("DiseaseName").replaceAll("\"","");
                Map<String, Object> data = gard.instrument(id);
                data.put("name", name);
                Entity ent = register (data);
                logger.info
                    ("+++++ "+data.get("id")+": "+name+" ("+ent.getId()+")");
            }
            logger.info(count+" entities registered!");
            ds.set(INSTANCES, count);
            updateMeta (ds);
        }
        
        return ds;
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
