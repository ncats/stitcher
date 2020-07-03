package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.util.zip.*;
import java.util.Base64;
import java.net.URL;
import java.sql.*;

import java.lang.reflect.Array;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.security.DigestInputStream;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigObject;
import com.zaxxer.hikari.HikariDataSource;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class NCGCEntityFactory extends MoleculeEntityFactory {
    static final Logger logger =
        Logger.getLogger(NCGCEntityFactory.class.getName());
    
    HikariDataSource dsource = new HikariDataSource ();
    int count;
    int maxrows;

    class SampleRegistration implements AutoCloseable {
        PreparedStatement pstm;
        PreparedStatement pstm2;
        InputStream is;
        int count;

        SampleRegistration (Connection con, int maxrows) throws Exception {
            pstm = con.prepareStatement
                ("select sample_id,smiles_iso,sample_name,supplier,supplier_id,"
                 +"pubchem_sid,pubchem_cid,cas,primary_moa,approval_status,"
                 +"tox21_id,sample_name2 "
                 +"from ncgc_sample "
                 +"where smiles_iso is not null "
                 +(maxrows > 0 ? "order by sample_id fetch first "
                   +maxrows+" rows only":""));
            pstm2 = con.prepareStatement
                ("select key,value from sample_annotation where sample_id = ?");
        }

        SampleRegistration (Connection con, InputStream is) throws Exception {
            pstm = con.prepareStatement
                ("select sample_id,smiles_iso,sample_name,supplier,supplier_id,"
                 +"pubchem_sid,pubchem_cid,cas,primary_moa,approval_status,"
                 +"tox21_id,sample_name2 "
                 +"from ncgc_sample "
                 +"where smiles_iso is not null and sample_id = ?");
            pstm2 = con.prepareStatement
                ("select key,value from sample_annotation where sample_id = ?");
            this.is = is;
        }

        int register () throws Exception {
            if (is != null) {
                try (BufferedReader br = new BufferedReader
                     (new InputStreamReader (is))) {
                    for (String line; (line = br.readLine()) !=  null; ) {
                        pstm.setString(1, line.trim());
                        register (pstm.executeQuery());
                    }
                }
            }
            else {
                register (pstm.executeQuery());
            }
            return count;
        }

        void register (ResultSet rset) throws Exception {
            Map<String, Object> row = new TreeMap<>();      
            while (rset.next()) {
                String sampleId = instrument (rset, row);
            
                System.out.println("+++++ "+sampleId+" "+(count+1)+" +++++");
                pstm2.setString(1, sampleId);
                ResultSet rs = pstm2.executeQuery();
                while (rs.next()) {
                    String key = rs.getString(1);
                    String val = rs.getString(2);
                    switch (key) {
                    case "library":
                        row.put("Library", Util.merge(row.get("Library"), val));
                        break;
                        
                    case "compound.name.primary":
                        row.put("SampleName",
                                Util.merge(row.get("SampleName"), val));
                        break;
                        
                    default:
                        logger.warning("Unknown sample annotation: "+key);
                    }
                }
                rs.close();
                
                ncats.stitcher.Entity e = registerIfAbsent (row);
                if (e != null)
                    ++count;
            }
            rset.close();
        }

        public void close () throws Exception {
            pstm.close();
            pstm2.close();
        }
    }
    
    public NCGCEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public NCGCEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public NCGCEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("SampleId");
        setStrucField ("SMILES");
        add (N_Name, "SampleName");
        add (I_CID, "PubChemCID");
        add (I_SID, "PubChemSID");
        add (I_CODE, "SampleId");
        add (I_CODE, "Tox21Id");
        add (I_CAS, "CAS");
        add (T_Keyword, "ApprovalStatus");
        add (T_Keyword, "Library");
        add (T_Keyword, "Supplier");
    }

    String instrument (ResultSet rset, Map<String, Object> row)
        throws SQLException {
        row.clear();
        String sampleId = rset.getString("sample_id");  
        
        row.put("SampleId", sampleId);
        long cid = rset.getLong("pubchem_cid");
        if (!rset.wasNull()) row.put("PubChemCID", cid);
        long sid = rset.getLong("pubchem_sid");
        if (!rset.wasNull()) row.put("PubChemSID", sid);
        String smiles = rset.getString("smiles_iso");
        if (smiles != null) row.put("SMILES", smiles);
        String cas = rset.getString("cas");
        if (cas != null) row.put("CAS", cas);
        String moa = rset.getString("primary_moa");
        if (moa != null) row.put("MOA", moa);
        String status = rset.getString("approval_status");
        if (status != null) row.put("ApprovalStatus", status);
        String supl = rset.getString("supplier");
        if (supl != null) row.put("Supplier", supl);
        String suplId = rset.getString("supplier_id");
        if (suplId != null) row.put("SupplierId", suplId);
        String name = rset.getString("sample_name");
        if (name != null) row.put("SampleName", name);
        name = rset.getString("sample_name2");
        if (name != null)
            row.put("SampleName", Util.merge(row.get("SampleName"), name));
        String tox21 = rset.getString("tox21_id");
        if (tox21 != null) row.put("Tox21Id", tox21);
        return sampleId;
    }
    
    int register (Connection con) throws Exception {
        int cnt;
        try (SampleRegistration reg = new SampleRegistration (con, maxrows)) {
            cnt = reg.register();
        }
        return cnt;
    }

    int register (Connection con, InputStream is) throws Exception {
        int cnt;
        try (SampleRegistration reg = new SampleRegistration (con, is)) {
            cnt = reg.register();
        }
        return cnt;
    }
    
    public int register (String config, String username, String password)
        throws Exception {
        Config conf = ConfigFactory.parseReader
            (new InputStreamReader (new FileInputStream (config)));
        if (!conf.hasPath("source")) {
            throw new IllegalArgumentException
                ("Configuration contains no \"source\" definition!");
        }
                
        //System.out.println("source:"+source);
        if (conf.hasPath("cache")) {
            String cache = conf.getString("cache");
            setCache (cache);
        }

        Config source = conf.getConfig("source");
        if (!source.hasPath("name")) {
            throw new IllegalArgumentException
                ("Configuration contains no \"source.name\" definition!");
        }
        
        if (!source.hasPath("url")) {
            throw new IllegalArgumentException
                ("Configuration contains no \"source.url\" definition!");
        }

        File file = null;
        if (source.hasPath("file")) {
            file = new File (source.getString("file"));
            if (!file.exists()) {
                logger.warning("File '"+file+"' doesn't exist!");
                return -1;
            }
            logger.info("### loading file..."+file);
        }
        else if (source.hasPath("rows")) {
            maxrows = source.getInt("rows");
        }
                
        dsource.setJdbcUrl(source.getString("url"));
        dsource.setUsername(username);
        dsource.setPassword(password);

        count = 0;
        try (Connection con = dsource.getConnection()) {
            DataSource ds = getDataSourceFactory()
                    .register(source.getString("name"));
            Integer instances = (Integer)ds.get(INSTANCES);
            if (instances != null && instances > 0) {
                logger.info("### Data source "+ds.getName()
                            +" has already been registered with "+instances
                            +" entities!");
            }
            setDataSource (ds);

            if (file != null) {
                count = register (con, new FileInputStream (file));
            }
            else {
                count = register (con);
            }
            
            if (count > 0) {
                if (instances == null) {
                    instances = 0;
                    updateMeta (ds);
                }
                ds.set(INSTANCES, instances+count);
            }
            
            logger.info("$$$ "+count+" new entities registered "
                        +"for data source \""+ds+"\"");
        }
        catch (SQLException ex) {
            ex.printStackTrace();
        }
        
        return count;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 4) {
            System.err.println("Usage: " + NCGCEntityFactory.class.getName()
                    + " DBDIR CONFIG USERNAME PASSWORD");
            System.exit(1);
        }
        
        NCGCEntityFactory ncgc = new NCGCEntityFactory (argv[0]);
        try {
            int count = ncgc.register(argv[1], argv[2], argv[3]);
        }
        finally {
            ncgc.shutdown();
        }
    }
}
