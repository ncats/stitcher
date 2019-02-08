package ncats.stitcher.impl;

import java.io.*;
import java.net.URL;
import java.net.HttpURLConnection;
import java.security.MessageDigest;
import java.util.*;
import java.text.*;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * sbt stitcher/"runMain ncats.stitcher.impl.FDAOrphanDesignationEntityFactory stitcher.db"
 */
public class FDAOrphanDesignationEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(FDAOrphanDesignationEntityFactory.class.getName());

    static final DateFormat DF = new SimpleDateFormat ("MM/dd/yyyy");

    static final String OOPD_URL = "https://www.accessdata.fda.gov/scripts/opdlisting/oopd/OOPD_Results.cfm";
    static final String OOPD_FORM = "Product_name=&Designation=&Designation_Start_Date=01%2F01%2F1983&Search_param=DESDATE&Output_Format=Excel&Sort_order=GENNAME&RecordsPerPage=25&newSearch=Run+Search&Designation_End_Date=";

    static class RowReader implements Iterator<Map<String, Object>> {
        final BufferedReader br;
        final String[] header;
        Map<String, Object> next;
        int rows;

        RowReader (InputStream is) throws Exception {
            br = new BufferedReader (new InputStreamReader (is));
            header = fetchNextRow ();
            nextRow ();
        }

        void nextRow () {
            next = null;
            String[] r = fetchNextRow ();
            if (r != null) {
                next = new TreeMap<>();
                for (int i = 0; i < header.length; ++i)
                    next.put(header[i], r[i]);
            }
        }
        
        String[] fetchNextRow () {
            List<String> row = new ArrayList<>();
            try {
                StringBuilder content = null;
                for (String line; (line = br.readLine()) != null;) {
                    //logger.info(line);
                    line = line.replaceAll("&nbsp;", " ");
                    if (line.indexOf("</tr>") > 0) {
                        ++rows;
                        break;
                    }
                    else if (line.indexOf("<td>") > 0
                             || line.indexOf("<th>") > 0) {
                        content = new StringBuilder ();
                    }
                    else if (line.indexOf("</td>") > 0
                             || line.indexOf("</th>") > 0) {
                        row.add(content.toString().trim());
                        content = null;
                    }
                    else if (content != null) {
                        content.append(line.trim()+"\n");
                    }
                }
            }
            catch (IOException ex) {
                logger.log(Level.SEVERE, "Can't parse input stream", ex);
            }
                            
            return row.isEmpty() ? null : row.toArray(new String[0]);
        }

        public boolean hasNext () {
            return next != null;
        }

        public Map<String, Object> next () {
            Map<String, Object> cur = next;
            nextRow ();
            return cur;
        }

        public int rows () { return rows; }
    }

    public FDAOrphanDesignationEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public FDAOrphanDesignationEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public FDAOrphanDesignationEntityFactory (GraphDb graphDb)
        throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        add(I_CODE, "DesignationCUI")
            .add(I_CODE, "ApprovedIndicationCUI")
            .add(T_Keyword, "FDA Approval Status")
            .add(T_Keyword, "Orphan Drug Status")
            ;
    }
    
    public DataSource register (InputStream is, String version)
        throws Exception {
        DataSource ds = getDataSourceFactory()
            .register("OrphanDrugDesignation_"+version);
        setDataSource (ds);

        RowReader rr = new RowReader (is);
        while (rr.hasNext()) {
            Map<String, Object> row = rr.next();
            /*
             * TODO: resolve row.get("Generic Name") and/or
             * row.get("Trade Name") for compounds and 
             * row.get("Designation") and/or row.get("Approved Indication")
             * for conditions!
             */
            System.out.println(String.format("%1$5d:", rr.rows())+" "+row);
        }
        
        return null;
    }
    
    public DataSource register () throws Exception {
        String date = DF.format(new Date ());
        URL url = new URL (OOPD_URL);
        HttpURLConnection http = (HttpURLConnection)url.openConnection();
        http.setRequestMethod("POST");
        http.setDoOutput(true);
        http.setRequestProperty("charset", "utf-8");
        http.setRequestProperty
            ("Content-Type", "application/x-www-form-urlencoded");
        byte[] data = (OOPD_FORM+date.replaceAll("/", "%2F")).getBytes("utf8");
        http.setRequestProperty("Content-Length", String.valueOf(data.length));
        http.getOutputStream().write(data, 0, data.length);
        return register (http.getInputStream(), date);
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "
                        +FDAOrphanDesignationEntityFactory.class.getName()
                        +" DBDIR");
            System.exit(1);
        }

        logger.info("Registering "+OOPD_URL+"...");
        try (FDAOrphanDesignationEntityFactory oopd =
             new FDAOrphanDesignationEntityFactory (argv[0])) {
            DataSource ds = oopd.register();
        }
    }
}
