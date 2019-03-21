package ncats.stitcher.impl;

import java.io.*;
import java.net.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.concurrent.Callable;
import java.lang.reflect.Array;

import java.sql.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;

import com.fasterxml.jackson.databind.ObjectMapper;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * data from ftp://ftp.ncbi.nlm.nih.gov/pub/medgen/
 */
public class MedGenEntityFactory extends EntityRegistry {
    static final Logger logger =
        Logger.getLogger(MedGenEntityFactory.class.getName());

    static abstract class MedGenReader
        implements Iterator<Map<String, Object>> {
        final String[] header;
        final LineTokenizer tokenizer;
        Map<String, String> row = new TreeMap<>();
        Map<String, Object> current;
        
        MedGenReader (File file) throws IOException {
            tokenizer = new LineTokenizer ('|');
            tokenizer.setInputStream
                (new GZIPInputStream (new FileInputStream (file)));
            if (tokenizer.hasNext()) {
                header = tokenizer.next();
                header[0] = header[0].substring(1); // skip #
            }
            else
                throw new IllegalArgumentException
                    ("Input file "+file+" is empty!");
            current = getNext ();            
        }

        public boolean hasNext () {
            return current != null;
        }

        public Map<String, Object> next () {
            Map<String, Object> next = current;
            current = getNext ();
            return next;
        }

        protected Map<String, Object> getNext () {
            Map<String, Object> rec = new TreeMap<>();
            
            updateRecord (rec);
            row.clear();
            while (tokenizer.hasNext()) {
                String[] toks = tokenizer.next();
                if (toks.length != header.length) {
                    logger.warning(tokenizer.getLineCount()+": expect "
                                   +header.length+" tokens but instead got "
                                   +toks.length);
                }
                else {
                    row.clear();
                    for (int i = 0; i < toks.length; ++i) {
                        if (header[i] != null) {
                            String val = toks[i];
                            if (val != null && val.length() > 32766)
                                val = val.substring(0, 32766);
                            row.put(header[i], val);
                        }
                    }

                    if (suppress ()) {
                    }
                    else if (rec.isEmpty()
                        || row.get("CUI").equals(rec.get("CUI"))) {
                        updateRecord (rec);
                    }
                    else {
                        break;
                    }
                }
            }
            
            return rec.isEmpty() ? null : rec;
        }

        protected abstract boolean suppress ();
        protected abstract Map<String, Object>
            updateRecord (Map<String, Object> rec);
    }

    static class StyReader extends MedGenReader {
        StyReader (File file) throws IOException {
            super (file);
        }

        protected boolean suppress () { return false; }
        protected Map<String, Object> updateRecord (Map<String, Object> rec) {
            for (Map.Entry<String, String> me : row.entrySet()) {
                if (rec.containsKey(me.getKey())
                    && !"CUI".equals(me.getKey())) {
                    rec.put(me.getKey(),
                            Util.merge(rec.get(me.getKey()), me.getValue()));
                }
                else {
                    rec.put(me.getKey(), me.getValue());
                }
            }
            return rec;
        }        
    }
    
    static class ConsoReader extends MedGenReader {
        ConsoReader (File file) throws IOException {
            super (file);
        }

        protected boolean suppress () {
            return "Y".equals(row.get("SUPPRESS"));
        }
        
        protected Map<String, Object> updateRecord (Map<String, Object> rec) {
            if (row.isEmpty())
                return rec;
            
            String cui = row.get("CUI");
            String name = null, syn = null;
            if ("P".equals(row.get("TS")) && "PF".equals(row.get("STT"))
                && "Y".equals(row.get("ISPREF")))
                name = row.get("STR");
            else
                syn = row.get("STR");
            String sab = row.get("SAB");
            String code = row.get("SDUI");
            if ("MSH".equals(sab)) code = "MESH:"+code;
            else if ("NCI".equals(sab)) code = "NCI:"+row.get("CODE");
            else if ("SNOMEDCT_US".equals(sab))
                code = sab+":"+row.get("CODE");
            else if ("ORDO".equals(sab)) {
                code = code.replaceAll("_", ":").toUpperCase();
            }
            
            if (rec.isEmpty()) {
                rec.put("CUI", cui);
                if (name != null) rec.put("NAME", name);
                else rec.put("SYNONYMS", syn);
                rec.put("XREFS", new String[]{
                        code, cui.startsWith("CN")
                        ? "MEDGEN:"+cui:"UMLS:"+cui});
                rec.put("SOURCES", sab);
            }
            else {
                if (rec.containsKey("NAME")) {
                    if (name != null)
                        rec.put("SYNONYMS",
                                Util.merge(rec.get("SYNONYMS"), name));
                    else
                        rec.put("SYNONYMS",
                                Util.merge(rec.get("SYNONYMS"), syn));
                }
                else if (name != null) {
                    rec.put("NAME", name);
                }
                else {
                    rec.put("SYNONYMS",
                            Util.merge(rec.get("SYNONYMS"), syn));
                }
                rec.put("XREFS", Util.merge
                        (rec.get("XREFS"), code, cui.startsWith("CN") ?
                         "MEDGEN:"+cui : "UMLS:"+cui));
                rec.put("SOURCES", Util.merge(rec.get("SOURCES"), sab));
            }
            return rec;
        }
    }
    

    public MedGenEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    public MedGenEntityFactory (String dir) throws IOException {
        super (dir);
    }
    public MedGenEntityFactory (File dir) throws IOException {
        super (dir);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("CUI");
        setNameField ("NAME");
        add (N_Name, "NAME")
            .add(N_Name, "SYNONYMS")
            .add(I_CODE, "XREFS")
            .add(T_Keyword, "TUI")
            .add(T_Keyword, "STY")
            .add(T_Keyword, "SOURCES")
            ;
    }

    public DataSource register (File dir, int version) throws IOException {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException
                ("Not a directory; please download the content of MedGen "
                 +"here ftp://ftp.ncbi.nlm.nih.gov/pub/medgen/!");
        }
        
        DataSource ds = getDataSourceFactory().register("MEDGEN_v"+version);
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        setDataSource (ds);
        
        File file = new File (dir, "MGCONSO.RRF.gz");
        if (!file.exists())
            throw new IllegalArgumentException
                ("Folder "+dir+" doesn't have file MGCONSO.RRF.gz!");
        ConsoReader conso = new ConsoReader (file);

        file = new File (dir, "MGSTY.RRF.gz");
        if (!file.exists())
            throw new IllegalArgumentException
                ("Folder "+dir+" doesn't have file MGSTY.RRF.gz!");
        StyReader sty = new StyReader (file);
        
        ObjectMapper mapper = new ObjectMapper ();
        Map<String, Object> t = null;
        int count = 0;
        while (conso.hasNext()) {
            Map<String, Object> r = conso.next();
            while (sty.hasNext()
                   && (t == null || (((String)t.get("CUI"))
                                     .compareTo((String)r.get("CUI")) < 0))) {
                t = sty.next();
            }
            
            if (r.get("CUI").equals(t.get("CUI"))) {
                r.putAll(t);
            }

            if (!r.containsKey("NAME")) {
                Object syn = r.get("SYNONYMS");
                if (syn.getClass().isArray())
                    syn = Array.get(syn, 0);
                r.put("NAME", syn);
            }

            try {
                Entity ent = register (r);
            
                ++count;            
                logger.info
                    ("+++++ "+String.format("%1$7d", count)+" "
                     +r.get("CUI")+": "+r.get("NAME")+" ("+ent.getId()+")");
            }
            catch (Exception ex) {
                logger.log(Level.SEVERE, ">>> "+mapper.valueToTree(r), ex);
                break;
            }
        }
        ds.set(INSTANCES, count);
        updateMeta (ds);
        
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            logger.info("Usage: "+MedGenEntityFactory.class.getName()
                        +" DBDIR MEDGEN_DIR");
            System.exit(1);
        }
        
        try (MedGenEntityFactory medgen = new MedGenEntityFactory (argv[0])) {
            medgen.register(new File (argv[1]), 1);
        }
    }
}
