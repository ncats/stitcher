package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

public class TCRDEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(TCRDEntityFactory.class.getName());

    public TCRDEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public TCRDEntityFactory (String dir) throws IOException {
        super (dir);
    }
    
    public TCRDEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("accession");
        add(I_GENE, "sym")
            .add(I_CODE, "xrefs")
            .add(I_CODE, "accession")
            .add(N_Name, "drugs")
            .add(T_Keyword, "tdl")
            ;
    }
    
    public Integer register (Connection con) throws Exception {
        int count = 0;
        try (Statement stm = con.createStatement()) {
            ResultSet rset = stm.executeQuery
                ("select uniprot,tdl,length(a.seq) as aalen, "
                 +"d.itype, d.integer_value, d.number_value, "
                 +"e.drug, a.sym, a.description "
                 +"from protein a, target b, t2tc c "
                 +"left join tdl_info as d on "
                 +"d.protein_id=c.protein_id "
                 +"left join drug_activity as e "
                 +"on e.target_id=c.target_id "
                 +"where a.id = c.protein_id and "
                 +"b.id = c.target_id order by a.id");
            String accession = null;
            Map<String, Object> row = new LinkedHashMap<>();
            while (rset.next()) {
                String acc = rset.getString("uniprot");
                if (!acc.equals(accession)) {
                    if (!row.isEmpty()) {
                        Entity e = register (row);
                        if (e != null) {
                            logger.info("+++ "+String.format("%1$05d", count)
                                        +": "+row.get("accession")
                                        +" "+row.get("tdl")+"...");
                            ++count;
                        }
                    }
                    row.clear();
                    String tdl = rset.getString("tdl");
                    row.put("accession", acc);
                    row.put("tdl", tdl);
                    row.put("name", rset.getString("description"));
                    row.put("sym", rset.getString("sym"));
                    row.put("xrefs", new String[]{
                            "UNIPROTKB:"+acc,
                            "REACTOME:"+acc,
                            "SWISSPROT:"+acc
                        });
                    row.put("aalen", rset.getInt("aalen"));
                }
                else {
                    String drug = rset.getString("drug");
                    String itype = rset.getString("itype");
                    int ival = rset.getInt("integer_value");
                    if (rset.wasNull()) {
                        float fval = rset.getFloat("number_value");
                        if (!rset.wasNull())
                            row.put(itype, fval);
                    }
                    else {
                        row.put(itype, ival);
                    }
                    if (drug != null) {
                        Object val = row.get("drugs");
                        row.put("drugs",
                                Util.merge(val, rset.getString("drug")));
                    }
                }
                accession = acc;
            }
            if (!row.isEmpty()) {
                Entity e = register (row);
                if (e != null) {
                    logger.info("+++ "+String.format("%1$05d", count)+": "
                                +row.get("accession")
                                +" "+row.get("tdl")+"...");
                    ++count;
                }
            }
        }
        return count > 0 ? count : null;
    }
    
    public DataSource register (String schema,
                                String username, String password)
        throws Exception {
        logger.info("registering "+schema+"...USER="+username);
        DataSource ds = null;
        try (Connection con = DriverManager.getConnection
             (schema, username, password)) {
            int pos = schema.lastIndexOf('/');
            ds = getDataSourceFactory().register
                (pos > 0 ? schema.substring(pos+1) : schema);
            setDataSource (ds);
            
            Integer instances = (Integer)ds.get(INSTANCES);
            if (instances != null) {
                logger.warning("### Data source "+ds.getName()
                               +" has already been registered with "+instances
                               +" entities!");
            }
            else {
                ds.set("url", schema);
                instances = register (con);
                if (instances != null) {
                    ds.set(INSTANCES, instances);
                    updateMeta (ds);
                    logger.info("$$$ "+instances
                                +" entities registered for "+ds);
                }
                else {
                    logger.warning("### No entities registered for "
                                   +"data source "+ds.getName());
                }
            }
        }
        return ds;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+TCRDEntityFactory.class.getName()
                        +" DBDIR SCHEMA_URL[e.g., "
                        +"jdbc:mysql://tcrd.ncats.io/tcrd660] "
                        +"[USER=tcrd] [PASSWORD='']");
            System.exit(1);
        }
        
        try (TCRDEntityFactory tcrd = new TCRDEntityFactory (argv[0])) {
            String user = "tcrd", password = "";
            if (argv.length > 2)
                user = argv[2];
            if (argv.length > 3)
                password = argv[3];
            DataSource ds = tcrd.register(argv[1], user, password);
        }
    }
}
