package ncats.stitcher.impl;

import java.util.*;
import java.util.zip.*;
import java.util.logging.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.util.regex.*;
import java.lang.reflect.Array;

import ncats.stitcher.GraphDb;
import ncats.stitcher.StitchKey;
import ncats.stitcher.Util;
import ncats.stitcher.DataSource;
import ncats.stitcher.impl.MapEntityFactory;
import ncats.stitcher.LineTokenizer;


public class DrugsAtFDA extends MapEntityFactory {
    static private final Logger logger =
        Logger.getLogger(DrugsAtFDA.class.getName());

    // expected files in drugs@FDA download
    static final String[] FILES = {
        "Products.txt",
        "MarketingStatus.txt",  
        "ApplicationDocs.txt",
        "Applications.txt",
        "Submissions.txt"
    };

    // this is the same as the file MarketingStatus_Lookup.txt
    static final String[] MarketingStatus = {
        "", // 0
        "Prescription", // 1
        "Over-the-counter", // 2
        "Discontinued", // 3
        "None (Tentative Approval)" // 4
    };

    // this is the samw as the file ApplicationsDocsType_Lookup.txt
    static final String[] DocumentType = {
        "", // 0
        "Letter", // 1
        "Label",
        "Review",
        "FDA Talk Paper",
        "FDA Press Release",
        "Patient Package Insert",
        "Dear Health Professional Letter",
        "Medication Guide",
        "Withdrawal Notice",
        "Other Important Information from FDA",
        "Consumer Information Sheet",
        "Exclusivity Letter",
        "Questions and Answers",
        "Other",
        "Patient Information Sheet",
        "Healthcare Professional Sheet",
        "Pediatric Summary, Medical Review",
        "Pediatric Summary, Clinical Pharmacology Review",
        "REMS",
        "Pediatric Summary, Clinical Pharmacology Review",
        "Summary Review",
            "Federal Register Notice" //dkatzel 2017-04-17- added missing type
    };

    // SubmissionClass_Lookup.txt
    static final String[][] SubmissionClass = {
        {"",""},
        {"BIOEQUIV","Bioequivalence"},
        {"EFFICACY","Efficacy"},
        {"LABELING","Labeling"},
        {"MANUF (CMC)","Manufacturing (CMC)"},
        {"N/A","Not Applicable"},
        {"S","Supplement"},
        {"TYPE 1","Type 1 - New Molecular Entity"},
        {"TYPE 1/4","Type 1 - New Molecular Entity and Type 4 - New Combination"},
        {"TYPE 2","Type 2 - New Active Ingredient"},
        {"TYPE 2/3","Type 2 - New Active Ingredient and Type 3 - New Dosage Form"},
        {"TYPE 2/4","Type 2 New Active Ingredient and Type 4 New Combination"},
        {"TYPE 3","Type 3 - New Dosage Form"},
        {"TYPE 3/4","Type 3 - New Dosage Form and Type 4 - New Combination"},
        {"TYPE 4","Type 4 - New Combination"},
        {"TYPE 5","Type 5 - New Formulation or New Manufacturer"},
        {"TYPE 6","Type 6 - New Indication (no longer used)"},
        {"TYPE 7","Type 7 - Drug Already Marketed without Approved NDA"},
        {"TYPE 8","Type 8 - Partial Rx to OTC Switch"},
        {"UNKNOWN",""},
        {"Unspecified",""},
        {"REMS","REMS"},
        {"TYPE 10","Type 10 - New Indication Submitted as Distinct NDA - Not Consolidated"},
        {"MEDGAS","Medical Gas"},
        {"TYPE 9","Type 9 - New Indication Submitted as Distinct NDA, Consolidated with Original NDA after Approval"},
        {"TYPE 9- BLA","Type 9 - New indication submitted as distinct BLA, consolidated"}
    };

    static final Pattern ProductRe;
    static {
        Pattern p = null;
        try {
            p = Pattern.compile("([^\\(]+)\\(([^\\)]+)");
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        ProductRe = p;
    }

    char delimiter = '\t';
    
    public DrugsAtFDA (GraphDb graphDb) throws IOException {
        super (graphDb);
    }
    
    public DrugsAtFDA (String dir) throws IOException {
        super (dir);
    }
    
    public DrugsAtFDA (File dir) throws IOException {
        super (dir);
    }

    public void setDelimiter (char delimiter) {
        this.delimiter = delimiter;
    }
    public char getDelimiter () { return delimiter; }

    @Override
    protected void init () {
        super.init();
        setIdField("ApplNo")
            .setNameField("ActiveIngredient")
            .add(StitchKey.N_Name, "ActiveIngredient");
    }

    protected String[] prodKeys = {"DrugName", "MarketingStatus", "SponsorName", "ApplType",
            "IngredientStrength", "ProductID", "EarliestDate", "ProdRoute", "ProdForm"}; // these last five are computed

    protected Map<String, ArrayList<String>> addProd(String ingredient, Map<String, ArrayList<String>> drug, String product, Map<String, Object> row) {
        for (String key: prodKeys) {
            if (!drug.containsKey(key))
                drug.put(key, new ArrayList<>());
            if (row.containsKey(key))
                drug.get(key).add(row.get(key).toString());
        }
        drug.get("ProductID").add(product);
        // compute earliest submission date
        if (row.containsKey("SubmissionStatusDate"))
            drug.get("EarliestDate").add(((String[])row.get("SubmissionStatusDate"))[0]);
        else {
            drug.get("EarliestDate").add("Not given");
        }
        // compute form
        if (row.containsKey("Form")) {
            String[] form = (String[])row.get("Form");
            drug.get("ProdForm").add(form[0]);
            if (form.length>1)
                drug.get("ProdRoute").add(form[1]);
            else
                drug.get("ProdRoute").add("Not given");
        } else {
            // code never gets here with current FDA file
            drug.get("ProdForm").add("Not given");
            drug.get("ProdRoute").add("Not given");
        }
        // compute ingredient strength
        if (((String[])row.get("ActiveIngredient")).length > 1) {
            String[] ingreds = (String[])row.get("ActiveIngredient");
            String[] strength = row.get("Strength").toString().split(";");
            int index = ingreds.length;
            if (strength.length == 1 && ingreds.length == 2) {
                strength = row.get("Strength").toString().split("/[\\d]");
            }
            if (strength.length < ingreds.length && ingreds.length > 2 && strength.length == 1 // Triple sulfas or TRISULFAPYRIMIDINES case
                    && ingreds[ingreds.length-1].trim().equals(ingredient)) {
                index = 0;
            } else if (strength.length > 1 || ingreds.length < 3 || strength.length == ingreds.length) {
                for (int i = 0; i < ingreds.length; i++)
                    if (ingreds[i].trim().equals(ingredient))
                        index = i;
            }
            if (index < strength.length)
                drug.get("IngredientStrength").add(strength[index].trim());
            else
                drug.get("IngredientStrength").add("Not found");
        } else {
            drug.get("IngredientStrength").add(row.get("Strength").toString());
        }
        return drug;
    }

    @Override
    public DataSource register (File file) throws IOException {
        try {
            Map<String, Map<String, Map<String, Object>>> data
                = new TreeMap<>();
            
            DataSource ds = file.isDirectory() ? registerFromDir (file, data)
                : registerFromZip (file, data);
            
            Integer instances = (Integer) ds.get(INSTANCES);
            if (instances != null) {
                logger.warning("### Data source "+ds.getName()
                               +" has already been registered with "+instances
                               +" entities!");
            }
            else {
                setDataSource (ds);

                // extract active ingredients / drugs

                Map<String, Map<String, ArrayList<String>>> drugs = new TreeMap<>();
                for (Map.Entry<String, Map<String, Map<String, Object>>> me
                        : data.entrySet()) {
                    for (String key : me.getValue().keySet()) {
                        Map<String, Object> row = me.getValue().get(key);
                        if (row.containsKey("ActiveIngredient")) {
                            for (String ingred: (String[])row.get("ActiveIngredient")) {
                                ingred = ingred.trim();
                                String product = me.getKey()+"-"+key;
                                if (drugs.containsKey(ingred)) {
                                    drugs.put(ingred, addProd(ingred, drugs.get(ingred), product, row));
                                } else {
                                    drugs.put(ingred, addProd(ingred, new TreeMap<>(), product, row));
                                }
                            }
                        }
                    }
                }


                int count = 0;
                for (String ingred: drugs.keySet()) {
                    System.out.println("++++++ "+ingred+" ++++++");
                    Map<String, ArrayList<String>> prodInfo = drugs.get(ingred);
                    Map<String, Object> drugData = new TreeMap<>();
                    drugData.put("ActiveIngredient", ingred);
                    for (String key: prodInfo.keySet()) {
                        drugData.put(key, prodInfo.get(key).toArray(new String[prodInfo.get(key).size()]));
                    }
                    if (count++ == 0) {
                        ds.set(PROPERTIES, drugData.keySet()
                                .toArray(new String[0]));
                        //System.out.println(me.getKey()+": "+row);
                    }
                    register (drugData);
                }
                ds.set(INSTANCES, count);
                updateMeta (ds);
                logger.info("$$$ "+count+" entities registered for "+ds);
            }
            
            return ds;
        }
        catch (Exception ex) {
            throw new RuntimeException (ex);
        }
    }

    /*
     * this must be load first
     */
    int loadProducts (InputStream is,
                      // appno->productno->data
                      Map<String, Map<String, Map<String, Object>>> data)
        throws IOException {
        int count = 0;  
        LineTokenizer tokenizer = new LineTokenizer (delimiter);
        tokenizer.setInputStream(is);
        
        String[] header = null;
        for (; tokenizer.hasNext(); ++count) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else {
                if (toks.length != header.length) {
                    logger.warning("Line "+tokenizer.getLineCount()
                                   +": expecting "+header.length
                                   +" fields but instead got "+toks.length);
                }
                
                Map<String, Map<String, Object>> app = data.get(toks[0]);
                if (app == null) {
                    data.put(toks[0], app = new HashMap<>());
                }

                Map<String, Object> row = app.get(toks[1]);
                if (row == null)
                    app.put(toks[1], row = new HashMap<>());
                
                for (int i = 0; i < Math.min(header.length, toks.length); ++i) {
                    if (toks[i] != null) {
                        String value = toks[i].trim();
                        if ("Form".equals(header[i])
                            || "ActiveIngredient".equals(header[i])) {
                            String[] vals = null;
                            
                            // there are two cases of this:
                            //   TRIPLE SULFA (SULFABENZAMIDE;SULFACETAMIDE;SULFATHIAZOLE)
                            Matcher m = ProductRe.matcher(value);
                            if (m.find()) {
                                String name = m.group(1).trim();
                                String[] comp = m.group(2).split(";");
                                // only consider the individual components if
                                //  they are separated by ;'s
                                if (comp.length > 1) {
                                    vals = new String[comp.length+1];
                                    for (int j = 0; j < comp.length; ++j)
                                        vals[j] = comp[j].trim();
                                    vals[comp.length] = name;
                                }
                            }

                            if (vals == null) {
                                vals = value.split(";");
                                for (int j = 0; j < vals.length; ++j)
                                    vals[j] = vals[j].trim();
                            }
                            
                            row.put(header[i], vals);
                        }
                        else
                            row.put(header[i], value);
                    }
                }
            }
        }
        
        logger.info(count+" products loaded!");
        return count;
    }

    int loadMarketingStatus (InputStream is,
                             Map<String, Map<String, Map<String, Object>>> data)
        throws IOException {
        int count = 0;
        LineTokenizer tokenizer = new LineTokenizer (delimiter);
        tokenizer.setInputStream(is);

        String[] header = null;
        for (; tokenizer.hasNext(); ++count) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else if (data.containsKey(toks[1])) {
                Map<String, Map<String, Object>> app = data.get(toks[1]);
                Map<String, Object> row = app.get(toks[2]);
                if (row == null)
                    app.put(toks[2], row = new HashMap<>());
                
                int status = Integer.parseInt(toks[0]);
                row.put("MarketingStatus", MarketingStatus[status]);
            }
        }
        logger.info(count+" marketing status loaded!");
        return count;
    }

    int loadApplications (InputStream is,
                          Map<String, Map<String, Map<String, Object>>> data)
        throws IOException {
        if (data.isEmpty())
            throw new IllegalArgumentException ("Input data is empty!");
            
        int count = 0;
        LineTokenizer tokenizer = new LineTokenizer (delimiter);
        tokenizer.setInputStream(is);

        String[] header = null;
        for (; tokenizer.hasNext(); ++count) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else if (data.containsKey(toks[0])) {
                Map<String, Map<String, Object>> products = data.get(toks[0]);
                for (Map<String, Object> p : products.values()) 
                    for (int i = 0; i < toks.length; ++i) 
                        if (toks[i] != null)
                            p.put(header[i], toks[i].trim());
            }
        }
        logger.info(count+" applications loaded!");
        return count;
    }
    
    int loadApplicationDocs
        (InputStream is, Map<String, Map<String, Map<String, Object>>> data)
        throws IOException {
        if (data.isEmpty())
            throw new IllegalArgumentException ("Input data is empty!");
        
        int count = 0;
        LineTokenizer tokenizer = new LineTokenizer (delimiter);
        tokenizer.setInputStream(is);

        //"ApplicationDocsID","ApplicationDocsTypeID","ApplNo","SubmissionType","SubmissionNo","ApplicationDocsTitle","ApplicationDocsURL","ApplicationDocsDate"
        String[] header = null;
        for (; tokenizer.hasNext(); ++count) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else if (data.containsKey(toks[2])) {
                Map<String, Map<String, Object>> products = data.get(toks[2]);
                for (Map<String, Object> p : products.values()) 
                    for (int i = 0; i < toks.length; ++i) 
                        if (toks[i] != null) {
                            if ("ApplicationDocsTypeID".equals(header[i])) {
                                p.put("ApplicationDocsType",
                                      DocumentType[Integer.parseInt
                                                   (toks[i])]);
                            }
                            p.put(header[i], toks[i].trim());
                        }
            }
        }
        logger.info(count+" application docs loaded!");
        return count;
    }

    // parallel array
    static void append (String key, String value, Map<String, Object> data) {
        Object ary = data.get(key);
        int len = 0;
        if (ary == null)
            data.put(key, ary = new String[1]);
        else if (!ary.getClass().isArray()) {
            String[] a = new String[2];
            a[0] = (String)ary;
            data.put(key, ary = a);
            len = 1;
        }
        else {
            len = Array.getLength(ary);
            String[] a = new String[len+1];
            for (int i = 0; i < len; ++i)
                a[i] = (String)Array.get(ary, i);
            data.put(key, ary = a);
        }
        
        Array.set(ary, len, value);
    }
    
    int loadSubmissions (InputStream is,
                         Map<String, Map<String, Map<String, Object>>> data)
        throws IOException {
        if (data.isEmpty())
            throw new IllegalArgumentException ("Input data is empty!");
        
        int count = 0;
        LineTokenizer tokenizer = new LineTokenizer (delimiter);
        tokenizer.setInputStream(is);

        //"ApplNo","SubmissionClassCodeID","SubmissionType","SubmissionNo","SubmissionStatus","SubmissionStatusDate","SubmissionsPublicNotes","ReviewPriority"
        String[] header = null;
        for (; tokenizer.hasNext(); ++count) {
            String[] toks = tokenizer.next();
            if (header == null) {
                header = toks;
            }
            else if (data.containsKey(toks[0])) {
                Map<String, Map<String, Object>> products = data.get(toks[0]);
                for (Map<String, Object> p : products.values()) 
                    for (int i = 1; i < toks.length; ++i) {
                        if (toks[i] != null) {
                            if ("SubmissionClassCodeID".equals(header[i])) {
                                append ("SubmissionClass",
                                        SubmissionClass[Integer.parseInt
                                                        (toks[i])][0], p);
                                append ("SubmissionClassDescription",
                                        SubmissionClass[Integer.parseInt
                                                        (toks[i])][1], p);
                            }
                            append (header[i], toks[i].trim(), p);
                        }
                        else {
                            append (header[i], "", p);
                        }
                    }
            }
        }
        logger.info(count+" submissions loaded!");
        return count;
    }

    void register (String file, InputStream is,
                   Map<String, Map<String, Map<String, Object>>> data,
                   MessageDigest md) throws Exception {
        DigestInputStream dis = new DigestInputStream (is, md);
        switch (file) {
        case "Products.txt":
            loadProducts (dis, data);
            break;
            
        case "MarketingStatus.txt":
            loadMarketingStatus (dis, data);
            break;
            
        case "Applications.txt":
            loadApplications (dis, data);
            break;
            
        case "ApplicationDocs.txt":
            loadApplicationDocs (dis, data);
            break;
            
        case "Submissions.txt":
            loadSubmissions (dis, data);
            break;
        }
        dis.close();
    }
    
    protected DataSource registerFromDir
        (File dir, Map<String, Map<String, Map<String, Object>>> data)
            throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA1");
        for (String file : FILES) {
            File f = new File (dir, file);
            if (!f.exists()) {
                throw new IllegalArgumentException
                    ("Input path "+dir.getName()+" doesn't have file: "+file);
            }
            else {
                register (file, new FileInputStream (f), data, md);
            }
        }

        return getDataSourceFactory().register
            (Util.sha1hex(md.digest()).substring(0,9), "Drugs@FDA", dir);
    }

    protected DataSource registerFromZip
        (File zipfile, Map<String, Map<String, Map<String, Object>>> data)
            throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA1");
        ZipFile zip = new ZipFile (zipfile);
        for (String file: FILES) {
            ZipEntry ze = zip.getEntry(file);
            if (ze == null)
                throw new IllegalArgumentException
                    ("Zip file doesn't have entry "+file);
            register (file, zip.getInputStream(ze), data, md);
        }
        
        return getDataSourceFactory().register
            (Util.sha1hex(md.digest()).substring(0,9), "Drugs@FDA", zipfile);
    }

    static void test () {
        Matcher m = ProductRe.matcher("TRIPLE SULFA (SULFABENZAMIDE;SULFACETAMIDE;SULFATHIAZOLE)");
        if (m.find()) {
            System.out.println(m.group(1));
            System.out.println(m.group(2));
        }
    }
        
    public static void main (String[] argv) throws Exception {
        if (argv.length < 2) {
            System.err.println
                ("Usage: ncats.stitcher.impl.DrugsAtFDA DB [delimiter=,|tab] (ZIPFILE|DIR)");
            System.exit(1);
        }

        //test ();
        
        DrugsAtFDA fda = new DrugsAtFDA (argv[0]);
        for (int i = 1; i < argv.length; ++i) {
            int pos = argv[i].indexOf('=');
            if (pos > 0) {
                if (argv[i].startsWith("delimiter")) {
                    String del = argv[i].substring(pos+1);
                    if ("tab".equalsIgnoreCase(del))
                        fda.setDelimiter('\t');
                    else
                        fda.setDelimiter(del.charAt(0));
                    logger.info("## delimiter: "+del);
                }
                else {
                    System.err.println("** Unknown option: "+argv[i]);
                }
            }
            else {
                logger.info("***** registering "+argv[i]+" ******");
                fda.register(new File (argv[i]));
            }
        }
        fda.shutdown();
    }
}
