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
        Map<String, String> row = new LinkedHashMap<>();
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

        public Map<String, Object> nextIfMatched (Map<String, Object> t,
                                                  Map<String, Object> r) {
            while (hasNext()
                   && (t == null || (((String)t.get("CUI"))
                                     .compareTo((String)r.get("CUI")) < 0))) {
                t = next ();
            }
            
            if (r.get("CUI").equals(t.get("CUI"))) {
                r.putAll(t);
            }

            return t;
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

    static class DefReader extends MedGenReader {
        DefReader (File file) throws IOException {
            super (file);
        }
        
        protected boolean suppress () {
            return "Y".equals(row.get("SUPPRESS"));
        }
        
        protected Map<String, Object> updateRecord (Map<String, Object> rec) {
            for (Map.Entry<String, String> me : row.entrySet()) {
                String key = me.getKey();
                if ("source".equals(key)) {
                    key = "SOURCE_DEF";
                }
                rec.put(key, me.getValue());
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
            else if ("OMIM".equals(sab)) {
                code = row.get("CODE");
                if (!code.startsWith("MTH"))
                    code = sab+":"+code;
            }
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

    static class RelReader extends MedGenReader {
        RelReader (File file) throws IOException {
            super (file);
        }

        @Override
        protected Map<String, Object> getNext () {
            Map<String, Object> r = null;
            if (tokenizer.hasNext()) {
                String[] toks = tokenizer.next();
                
                row.clear();
                for (int i = 0; i < toks.length; ++i) {
                    if (header[i] != null) {
                        String val = toks[i];
                        if (val != null && val.length() > 32766)
                            val = val.substring(0, 32766);
                        row.put(header[i], val);
                    }
                }
                
                r = new LinkedHashMap<>();
                for (Map.Entry<String, String> me : row.entrySet())
                    if (me.getValue() != null)
                        r.put(me.getKey(), me.getValue());
            }
            return r;
        }

        protected boolean suppress () {
            return "Y".equals(row.get("SUPPRESS"));
        }

        protected Map<String, Object> updateRecord (Map<String, Object> rec) {
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

    protected int registerEntities (File dir) throws IOException {
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

        file = new File (dir, "MGDEF.RRF.gz");
        if (!file.exists())
            throw new IllegalArgumentException
                ("Folder "+dir+" doesn't have file MGDEF.RRF.gz!");
        DefReader def = new DefReader (file);
        
        ObjectMapper mapper = new ObjectMapper ();
        Map<String, Object> t = null, d = null;
        int count = 0;
        while (conso.hasNext()) {
            Map<String, Object> r = conso.next();
            
            t = sty.nextIfMatched(t, r);
            d = def.nextIfMatched(d, r);
            
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
            //if (count > 1000) break;
        }
        return count;
    }

    protected int registerRelationships (File dir) throws IOException {
        File file = new File (dir, "MGREL.RRF.gz");
        if (!file.exists())
            throw new IllegalArgumentException
                ("Folder "+dir+" doesn't have file MGREL.RRF.gz!");
        RelReader reader = new RelReader (file);
        int count = 0;
        ObjectMapper mapper = new ObjectMapper ();
        while (reader.hasNext()) {
            Map<String, Object> r = reader.next();
            String cui1 = (String)r.get("CUI1");
            String cui2 = (String)r.get("CUI2");
            if (!cui1.equals(cui2)) {
                String rel = (String) r.get("REL");
                String rela = (String) r.get("RELA");
                List<Entity> targets = getEntities (I_CODE, "UMLS:"+cui1);
                List<Entity> sources = getEntities (I_CODE, "UMLS:"+cui2);
                for (Entity s : sources) {
                    for (Entity t : targets) {
                        if (!s.equals(t)) {
                            try {
                                if ("isa".equals(rela))
                                    s.stitch(t, R_subClassOf, r);
                                else
                                    s.stitch(t, R_rel,
                                             rela != null ? rela : rel, r);
                            }
                            catch (Exception ex) {
                                logger.log(Level.SEVERE,
                                           "Can't create relationship between "
                                           +cui1+" and "+cui2+"\n>>> "
                                           +mapper.valueToTree(r), ex);
                                return count;
                            }
                        }
                    }
                }
                
                ++count;
            }
        }
        return count;
    }

    protected int registerOMIMRelationships (File dir) throws IOException {
        File file = new File (dir, "MedGen_HPO_OMIM_Mapping.txt.gz");
        if (!file.exists())
            throw new IllegalArgumentException
                ("Folder "+dir
                 +" doesn't have file MedGen_HPO_OMIM_Mapping.txt.gz!");
        RelReader reader = new RelReader (file);
        int count = 0;
        while (reader.hasNext()) {
            Map<String, Object> r = reader.next();
            String src = (String) r.get("OMIM_CUI");
            String tar = (String) r.get("HPO_CUI");
            String rel = (String) r.get("relationship");
            if (rel != null && !src.equals(tar)) {
                List<Entity> targets = getEntities (I_CODE, "UMLS:"+src);
                List<Entity> sources = getEntities (I_CODE, "UMLS:"+tar);
                for (Entity s : sources) {
                    for (Entity t : targets) {
                        if (!s.equals(t))
                            s.stitch(t, R_rel, rel, r);
                    }
                }
                ++count;
            }
        }
        return count;
    }

    protected int registerOMIMGeneMedGen (File dir) throws IOException {
        File file = new File (dir, "mim2gene_medgen");
        if (!file.exists()) {
            logger.warning("No OMIM gene mapping to MedGen file found: "+file);
            return 0;
        }

        try (InputStream is = new FileInputStream (file)) {
            LineTokenizer tokenizer = new LineTokenizer ('\t');
            tokenizer.setInputStream(is);
            if (!tokenizer.hasNext()) {
                logger.warning("Empty file: "+file);
                return 0;
            }
            
            //#MIM number     GeneID  type    Source  MedGenCUI       Comment
            String[] header = tokenizer.next();
            header[0] = header[0].substring(1);
            
            Map<String, Object> row = new LinkedHashMap<>();
            int count = 0;
            while (tokenizer.hasNext()) {
                String[] toks = tokenizer.next();
                if (!"-".equals(toks[1]) && toks[4].startsWith("C")) {
                    row.clear();
                    for (int i = 0; i < header.length; ++i)
                        row.put(header[i], toks[i].trim());
                    //logger.info(">> "+row);
                    
                    String gid = "GENE:"+ toks[1];
                    List<Entity> omim = getEntities (I_CODE, "OMIM:"+toks[0]);
                    List<Entity> gene = getEntities (I_CODE, gid);
                    List<Entity> cui = getEntities (I_CODE, "UMLS:"+toks[4]);
                    for (Entity a : omim) {
                        for (Entity b : cui) {
                            if (!a.equals(b)) {
                                for (Entity c : gene) {
                                    a.stitch(c, I_GENE, gid);
                                    b.stitch(c, I_GENE, gid);
                                }
                                a.stitch(b, I_GENE, gid, row);
                            }
                        }
                    }
                    ++count;
                }
            }
            return count;
        }
    }
    
    List<Entity> getEntities (StitchKey key, Object value) {
        Iterator<Entity> iter = find (key.name(), value);
        List<Entity> entities = new ArrayList<>();
        while (iter.hasNext())
            entities.add(iter.next());
        return entities;
    }
    
    public DataSource register (File dir) throws IOException {
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException
                ("Not a directory; please download the content of MedGen "
                 +"here ftp://ftp.ncbi.nlm.nih.gov/pub/medgen/!");
        }
        
        DataSource ds = getDataSourceFactory().register("MEDGEN");
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        setDataSource (ds);

        int count = registerEntities (dir);
        if (count > 0) {
            int nrels = registerRelationships (dir);
            logger.info("#### "+nrels+" relationships registered!");
            nrels = registerOMIMRelationships (dir);
            logger.info("#### "+nrels+" OMIM relationships registered!");
            nrels = registerOMIMGeneMedGen (dir);
            logger.info("#### "+nrels
                        +" OMIM-GENE-MEDGE relationships registered!");
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
            medgen.register(new File (argv[1]));
            //medgen.registerOMIMGeneMedGen(new File (argv[1]));
        }
    }
}
