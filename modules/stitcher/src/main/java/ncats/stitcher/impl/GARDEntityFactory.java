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
 * sbt stitcher/"runMain ncats.stitcher.impl.GARDEntityFactory DBDIR"
 */
public class GARDEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(GARDEntityFactory.class.getName());

    static final String DEFAULT_JDBC_AUTH =
        "integratedSecurity=true;authenticationScheme=JavaKerberos";
    static final String GARD_JDBC =
        "jdbc:sqlserver://ncatswnsqldvv02.nih.gov;databaseName=ORDRGARD_DEV;";

    static class GARD_Old implements AutoCloseable {
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
        
        GARD_Old (Connection con) throws SQLException {
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
                    if (s != null && s.length() > 3)
                        names.add(s.replaceAll("\"", "").trim());
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

    static class GARD implements AutoCloseable {
        PreparedStatement pstm1, pstm2, pstm3, pstm4;
        
        Map<Integer, String> idtypes = new TreeMap<>();
        Map<Integer, String> diseasetypes = new TreeMap<>();
        Map<Integer, String> resources = new TreeMap<>();
        
        GARD (Connection con) throws SQLException {
            pstm1 = con.prepareStatement
                ("select * from RD_tblDiseaseIdentifiers "
                 +"where DiseaseID=? and IsActive = 1");
            pstm2 = con.prepareStatement
                ("select * from rd_tbldiseaselevel where DiseaseID = ?");
            pstm3 = con.prepareStatement
                ("select a.Question,a.Answer,c.ResourceClassificationName "
                 +"from TBLQuestion a, ASCQuestionClassification b, "
                 +"RD_tblResourceClassification c,vw_DiseaseQuestions d "
                 +"where a.QuestionID=d.QuestionID "
                 +"and a.QuestionID=b.QuestionID "
                 +"and b.ResourceClassificationID=c.ResourceClassificationID "
                 +"and d.DiseaseID=?");
            pstm4 = con.prepareStatement
                ("select * from RD_ascDiseaseTypes where DiseaseID=?");
            
            Statement stm = con.createStatement();
            ResultSet rset = stm.executeQuery
                ("select * from RD_luIdentifierType");
            while (rset.next()) {
                int id = rset.getInt("IdentifierTypeID");
                String type = rset.getString("IdentifierTypeText");
                idtypes.put(id, type);
            }
            rset.close();
            
            rset = stm.executeQuery("select * from RD_luDiseaseType");
            while (rset.next()) {
                int id = rset.getInt("DiseaseTypeID");
                String type = rset.getString("DiseaseTypeName");
                diseasetypes.put(id, type);
            }
            rset.close();

            rset = stm.executeQuery
                ("select * from RD_tblResourceClassification");
            while (rset.next()) {
                int id = rset.getInt("ResourceClassificationID");
                String name = rset.getString("ResourceClassificationName");
                resources.put(id, name);
            }
            rset.close();
        }

        public Map<String, Object> instrument (long id) throws SQLException {
            Map<String, Object> data = new TreeMap<>();
            data.put("gard_id", String.format("GARD:%1$05d", id));
            data.put("id", id);
            identifiers (data);
            categories (data);
            resources (data);
            relationships (data);
            return data;
        }

        Map<String, Object> identifiers (Map<String, Object> data)
            throws SQLException {
            pstm1.setLong(1, (Long)data.get("id"));
            
            ResultSet rset = pstm1.executeQuery();
            List<String> xrefs = new ArrayList<>();
            List<String> synonyms = new ArrayList<>();
            while (rset.next()) {
                int tid = rset.getInt("IdentifierTypeID");
                String value = rset.getString("DiseaseIdentifier");
                String type = idtypes.get(tid);
                if (type != null) {
                    if ("synonym".equalsIgnoreCase(type)) {
                        synonyms.add(value);
                    }
                    else {
                        xrefs.add(type.toUpperCase()+":"+value);
                    }
                }
                else {
                    logger.warning("Unknown identifier type: "+tid);
                }
            }
            rset.close();
            
            if (!xrefs.isEmpty())
                data.put("xrefs", xrefs.toArray(new String[0]));
            
            if (!synonyms.isEmpty())
                data.put("synonyms", synonyms.toArray(new String[0]));
            
            return data;
        }

        Map<String, Object> categories (Map<String, Object> data)
            throws SQLException {
            pstm4.setLong(1, (Long)data.get("id"));
            ResultSet rset = pstm4.executeQuery();
            List<String> categories = new ArrayList<>();
            while (rset.next()) {
                int id = rset.getInt("DiseaseTypeID");
                String type = diseasetypes.get(id);
                if (type != null) {
                    categories.add(type);
                }
            }
            rset.close();
            if (!categories.isEmpty()) {
                data.put("categories", categories.size() == 1
                         ? categories.get(0)
                         : categories.toArray(new String[0]));
            }
            return data;
        }

        Map<String, Object> resources (Map<String, Object> data)
            throws SQLException {
            pstm3.setLong(1, (Long)data.get("id"));
            ResultSet rset = pstm3.executeQuery();
            Map<String, List<String>> ans = new TreeMap<>();
            while (rset.next()) {
                String res = rset.getString("ResourceClassificationName");
                List<String> vals = ans.get(res);
                if (vals == null)
                    ans.put(res, vals = new ArrayList<>());
                vals.add(rset.getString("Answer"));
            }
            rset.close();
            
            for (Map.Entry<String, List<String>> me : ans.entrySet()) {
                List<String> vals = me.getValue();
                if (vals.size() == 1) {
                    data.put(me.getKey(), vals.get(0));
                }
                else {
                    data.put(me.getKey(), vals.toArray(new String[0]));
                }
            }
            
            return data;
        }

        Map<String, Object> relationships (Map<String, Object> data)
            throws SQLException {
            pstm2.setLong(1, (Long)data.get("id"));
            ResultSet rset = pstm2.executeQuery();
            Set<Long> parents = new TreeSet<>();
            while (rset.next()) {
                long parent = rset.getLong("ParentDiseaseID");
                if (!rset.wasNull())
                    parents.add(parent);
            }
            rset.close();
            
            if (!parents.isEmpty()) {
                if (parents.size() == 1)
                    data.put("parents", parents.iterator().next());
                else
                    data.put("parents", parents.toArray(new Long[0]));
            }
            return data;
        }
        
        public void close () throws Exception {
            pstm1.close();
            pstm2.close();
            pstm3.close();
            pstm4.close();
        }
    } // GARD

    
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
            .add(I_CODE, "xrefs")
            .add(T_Keyword, "categories")
            .add(T_Keyword, "sources")
            ;
    }
        
    public DataSource register (Connection con, int version)
        throws Exception {
        DataSource ds = getDataSourceFactory().register("GARD_v"+version);
        
        Statement stm = con.createStatement();        
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        
        setDataSource (ds);
        try (ResultSet rset = stm.executeQuery
             ("select * from RD_tblDisease where StatusID=4 and isSpanish=0 "
              +"and isRare = 1");
             GARD gard = new GARD (con)) {
            int count = 0;
            Map<Long, Entity> entities = new HashMap<>();
            Map<Long, Object> parents = new HashMap<>();
            for (; rset.next(); ++count) {
                long id = rset.getLong("DiseaseID");
                String name = rset.getString("DiseaseName");
                Map<String, Object> data = gard.instrument(id);
                data.put("name", name);
                Entity ent = register (data);
                logger.info
                    ("+++++ "+data.get("id")+": "+name+" ("+ent.getId()+")");
                if (data.containsKey("parents")) {
                    parents.put(id, data.get("parents"));
                }
                entities.put(id, ent);
            }
            
            // now setup relationships
            for (Map.Entry<Long, Object> me : parents.entrySet()) {
                Entity ent = entities.get(me.getKey());
                Object pval = me.getValue();
                if (pval.getClass().isArray()) {
                    int len = Array.getLength(pval);
                    for (int i = 0; i < len; ++i) {
                        Long id = (Long)Array.get(pval, i);
                        Entity p = (Entity)entities.get(id);
                        if (p != null)
                            ent.stitch(p, R_subClassOf, "GARD:"+id);
                        else
                            logger.warning("Can't find parent entity: "+id);
                    }
                }
                else {
                    Long id = (Long)pval;
                    Entity p = (Entity)entities.get(id);
                    if (p != null)
                        ent.stitch(p, R_subClassOf, "GARD:"+id);
                    else
                        logger.warning("Can't find parent entity: "+id);
                }
            }
            logger.info(count+" entities registered!");
            ds.set(INSTANCES, count);
            updateMeta (ds);
        }
        
        return ds;
    }

    public void dump (int version) {
        int count = maps (e -> {
                Map<String, Object> data = e.payload();
                System.out.println(data.get("id")+": "+data.get("name"));
            }, "GARD_v"+version);
        logger.info("####### "+count+" entries!");
    }

    public static class Register {
        public static void main (String[] argv) throws Exception {
            if (argv.length < 1) {
                logger.info("Usage: "+Register.class.getName()
                            +" DBDIR [user=USERNAME] [password=PASSWORD]"
                            +" [VERSION=1,2,..]");
                System.exit(1);
            }

            int version = 1;
            String user = null, password = null;
            for (int i = 1; i < argv.length; ++i) {
                if (argv[i].startsWith("user=")) {
                    user = argv[i].substring(5);
                }
                else if (argv[i].startsWith("password=")) {
                    password = argv[i].substring(9);
                }
                else if (argv[i].startsWith("version=")) {
                    version = Integer.parseInt(argv[i].substring(8));
                }
            }

            String jdbcUrl = GARD_JDBC;
            if (user != null && password != null) {
                jdbcUrl += "user="+user+";password="+password;
                logger.info("#### using credential "+user);
            }
            else {
                jdbcUrl += DEFAULT_JDBC_AUTH;
                logger.info("#### using default credentials");
            }
            
            try (GARDEntityFactory gef = new GARDEntityFactory (argv[0]);
                 Connection con = DriverManager.getConnection(jdbcUrl)) {
                gef.register(con, version);
            }
        }
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+GARDEntityFactory.class.getName()
                        +" DBDIR");
            System.exit(1);
        }
        
        try (GARDEntityFactory gef = new GARDEntityFactory (argv[0])) {
            gef.dump(1);
        }
    }
}
