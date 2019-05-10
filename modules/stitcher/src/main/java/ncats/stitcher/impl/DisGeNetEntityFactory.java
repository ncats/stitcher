package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.zip.GZIPInputStream;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * DisGeNet
 */
public class DisGeNetEntityFactory extends EntityRegistry {
    static final String DISGENET_URL =
        "http://www.disgenet.org/static/disgenet_ap1/files/downloads/curated_gene_disease_associations.tsv.gz";
    
    static final Logger logger =
        Logger.getLogger(DisGeNetEntityFactory.class.getName());

    public DisGeNetEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public DisGeNetEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public DisGeNetEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setNameField ("diseaseName");
        add (N_Name, "diseaseName")
            .add(I_GENE, "geneSymbol")
            .add(v -> Collections.singletonMap(I_GENE, "GENE:"+v), "geneId")
            .add(v -> Collections.singletonMap(I_CODE, "UMLS:"+v), "diseaseId")
            .add(v -> Collections.singletonMap
                 (T_Keyword, v.toString().split(";")), "source")
            ;
    }

    public DataSource register () throws Exception {
        DataSource ds = getDataSourceFactory().register("DisGeNet");
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        setDataSource (ds);

        File tmp = File.createTempFile("disgenet", "gz");
        MessageDigest md = MessageDigest.getInstance("sha1");
        URL url = new URL (DISGENET_URL);
        try (InputStream is = new DigestInputStream (url.openStream(), md);
             OutputStream os = new FileOutputStream (tmp)) {
            logger.info("#### Downloading "+DISGENET_URL+"...");
            byte[] buf = new byte[1024];
            size = 0;
            for (int nb; (nb = is.read(buf, 0, buf.length)) != -1; ) {
                os.write(buf, 0, nb);
                size += nb;
            }
            logger.info("#### "+size+" byte(s) downloaded into "+tmp);
        }

        try (InputStream is = new GZIPInputStream (new FileInputStream (tmp))) {
            LineTokenizer tokenizer = new LineTokenizer ();
            tokenizer.setInputStream(is);
            
            String[] header = null;
            int count = 0, lines = 0;
            Map<String, Object> data = new LinkedHashMap<>();
            for (; tokenizer.hasNext(); ++lines) {
                String[] toks = tokenizer.next();
                if (header == null) {
                    header = toks;
                }
                else if (header.length != toks.length) {
                    logger.warning(lines+": Expecting "+header.length
                                   +" but instead got "+toks.length
                                   +" columns!");
                }
                else {
                    data.clear();
                    for (int i = 0; i < header.length; ++i) {
                        if (toks[i] == null)
                            continue;
                        
                        String v = toks[i].trim();
                        switch (header[i]) {
                        case "YearInitial":
                        case "YearFinal":
                        case "NofPmids":
                        case "NofSnps":
                            try {
                                data.put(header[i], Integer.parseInt(v));
                            }
                            catch (NumberFormatException ex) {
                                logger.log(Level.SEVERE, "Not a number: "
                                           +v, ex);
                            }
                        break;
                        
                        case "diseaseClass":
                            data.put(header[i], v.split(";"));
                            break;
                        
                        case "score":
                        case "DSI":
                        case "DPI":
                        case "EI":
                            try {
                                data.put(header[i], Double.parseDouble(v));
                            }
                            catch (NumberFormatException ex) {
                                logger.log(Level.SEVERE, "Not a decimal: "
                                           +v, ex);
                            }
                        break;
                        
                        default:
                            data.put(header[i], v);
                        }
                    }
                    
                    Entity e = register (data);
                    ++count;
                    logger.info("++++++++ "+count+": "+data.get("geneSymbol")
                                +" <> "+data.get("diseaseName") +" => "
                                +e.getId());
                }
            }
            tmp.delete();
            logger.info(count+" entities registered!");
            
            ds.set(INSTANCES, count);
            { byte[] sha1 = md.digest();
                StringBuilder buf = new StringBuilder ();
                for (int i = 0; i < sha1.length; ++i)
                    buf.append(String.format("%1$02x", sha1[i] & 0xff));
                ds.set(SHA1, buf.toString());
            }
            updateMeta (ds);
        }
        return ds;
    }
    
    public static void main (String[] argv) throws Exception {
        if (argv.length == 0) {
            logger.info("Usage: "+DisGeNetEntityFactory.class.getName()
                        +" DBDIR");
            System.exit(1);
        }
        
        try (DisGeNetEntityFactory dis = new DisGeNetEntityFactory (argv[0])) {
            dis.register();
        }
    }
}

