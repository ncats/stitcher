package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.util.logging.Logger;
import java.util.logging.Level;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.*;
import javax.swing.text.html.parser.*;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * livertox site
 */
public class LiverToxEntityFactory extends EntityRegistry {
    static final String BASE_URL =
        "https://livertox.nlm.nih.gov/php/searchchem.php?chemrang=";
    static final String[] PAGES = {
        "A",
        "B",
        "C",
        "D",
        "E",
        "F",
        "G",
        "H",
        "I",
        "J",
        "K",
        "L",
        "M",
        "N",
        "O",
        "P",
        "Q",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z"
    };
        
    static final Logger logger =
        Logger.getLogger(NORDEntityFactory.class.getName());
    
    static class Callback extends HTMLEditorKit.ParserCallback {
        LinkedList<HTML.Tag> stack = new LinkedList<>();

        @Override
        public void handleStartTag (HTML.Tag tag,
                                    MutableAttributeSet attrs, int pos) {
            //logger.info("start: "+tag);
        }

        @Override
        public void handleEndTag (HTML.Tag tag, int pos) {
            //logger.info("end: "+tag);
        }
    }

    static class LiverToxCallback extends Callback {
        String id, href, text;
        Map<String, String> urls = new LinkedHashMap<>();
        
        @Override
        public void handleSimpleTag (HTML.Tag tag,
                                     MutableAttributeSet attrs, int pos) {
            boolean endtag = false;
            Map<String, String> params = new TreeMap<>();
            for (Enumeration en = attrs.getAttributeNames();
                 en.hasMoreElements(); ) {
                Object a = en.nextElement();
                if ("endtag".equals(a.toString())) {
                    endtag = true;
                }
                else {
                    params.put(a.toString(), attrs.getAttribute(a).toString());
                }
            }

            switch (tag.toString()) {
            case "div":
                id = endtag ? null : params.get("id");
                break;
            case "a":
                if ("Content".equals(id)) {
                    if (!endtag) {
                        href = params.get("href");
                        if (!href.startsWith("http"))
                            href = null;
                    }
                    else if (href != null) {
                        logger.info(text+": "+href);
                        urls.put(text, href.replaceAll("[\\s\n]+", ""));
                        href = null;
                    }
                }
                break;
            }
            
            if (endtag) {
                HTML.Tag t = stack.pop();
                text = null;
            }
            else {
                stack.push(tag);
            }            
        }
        
        @Override
        public void handleText (char[] chrs, int pos) {
            String tag = null;
            if (!stack.isEmpty())
                tag = stack.peek().toString();
            
            text = new String (chrs);
            //logger.info(tag+": <"+text+">");
        }
    }

    static class LiverToxPageCallback extends Callback {
        String id, section;
        Map<String, String> row;
        int col;
        List<String> cols = new ArrayList<>();
        StringBuilder text = new StringBuilder ();
        StringBuilder html = new StringBuilder ();
        boolean done = false;
        
        @Override
        public void handleSimpleTag (HTML.Tag tag,
                                     MutableAttributeSet attrs, int pos) {
            if (done) return;
            
            boolean endtag = false;
            Map<String, String> params = new TreeMap<>();
            for (Enumeration en = attrs.getAttributeNames();
                 en.hasMoreElements(); ) {
                Object a = en.nextElement();
                if ("endtag".equals(a.toString())) {
                    endtag = true;
                }
                else {
                    params.put(a.toString(), attrs.getAttribute(a).toString());
                }
            }

            String name = params.get("name");
            switch (tag.toString()) {
            case "div":
                if (!endtag) {
                    id = params.get("id");
                    if ("Content".equals(id)) {
                        text.setLength(0);
                        done = false;
                    }
                    else if ("footer".equals(id))
                        done = true;
                }
                break;
                
            case "strong":
                if (endtag) {
                    String heading = text.toString().trim();
                    //System.out.println("<<<"+heading+">>>");
                }
                break;
                
            case "a":
                if (!params.containsKey("href") && name != null)
                    section = name;
                break;
                
            case "tr":
                if (endtag) {
                    col = 0;
                    text.append("\n");
                }
                else {
                    id = params.get("id");
                    if (id != null) {
                        row = new LinkedHashMap<>();
                        cols.clear();
                    }
                }
                break;
                
            case "th":
                if (endtag) {
                    cols.add(html.toString());
                    text.append("\t");
                }
                break;
                
            case "td":
                if (endtag) {
                    if ("structure".equals(section)) {
                        String c = cols.get(col);
                        //logger.info(col+": "+c+": "+html);
                        row.put(c, html.toString());
                        ++col;
                    }
                    text.append("\t");
                }
                break;

            case "h2":
                if (endtag || (text.length() > 0
                               && text.charAt(text.length()-1) != '\n'))
                    text.append("\n");
                // fall through
                
            case "h3":
                if (endtag || (text.length() > 0
                               && text.charAt(text.length()-1) != '\n'))
                    text.append("\n");
                // fall through
                
            case "p":
            case "li":
                if (endtag) {
                    text.append("\n");
                }
                break;
                
            case "br":
                text.append("\n");
                break;

            case "table":
                if (!endtag) text.append("\n");
            }
            html.setLength(0);
        }

        @Override
        public void handleText (char[] chrs, int pos) {
            if (done) return;
            
            String tag = null;
            if (!stack.isEmpty())
                tag = stack.peek().toString();

            String s = new String (chrs);
            s = s.replaceAll("&nbsp;", "").replaceAll("&quot;", "\"")
                .replaceAll("Top of page", "").replaceAll("&ndash;", "--")
                .replaceAll("&reg;", "(r)").replaceAll("DRUG RECORD", "");
            
            text.append(s);
            html.append(s);
            //logger.info(tag+": <"+text+">");
        }

        void clear () {
            text.setLength(0);
            html.setLength(0);
            id = null;
            section = null;
            cols.clear();
        }
    }
    
    public LiverToxEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public LiverToxEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public LiverToxEntityFactory (GraphDb graphDb) throws IOException {
        super (graphDb);
    }

    @Override
    protected void init () {
        super.init();
        setIdField ("name");
        setNameField ("name");
        add (N_Name, "name")
            .add(N_Name, "synonyms")
            ;
    }
    
    public DataSource register () throws Exception {
        DataSource ds = getDataSourceFactory().register("LiverTox");
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        setDataSource (ds);
        
        DocumentParser parser = new DocumentParser (DTD.getDTD(""));
        LiverToxCallback livertox = new LiverToxCallback ();
        for (String page : PAGES) {
            URL url = new URL (BASE_URL+page);
            logger.info("#### processing "+url);
            parser.parse(new InputStreamReader
                         (url.openStream()), livertox, true);
        }
        logger.info(livertox.urls.size()+" pages!");

        File dir = new File ("livertox");
        dir.mkdirs();
        
        for (Map.Entry<String, String> me : livertox.urls.entrySet()) {
            logger.info(me.getKey());
            URL url = new URL (me.getValue());
            logger.info("#### processing "+me.getValue());
            LiverToxPageCallback callback = new LiverToxPageCallback ();
            parser.parse(new InputStreamReader
                         (url.openStream()), callback, true);
            //logger.info("*** "+callback.row);
            //logger.info("*** "+callback.text);
            File out = new File (dir, me.getKey()+".txt");
            PrintStream ps = new PrintStream (new FileOutputStream (out));
            ps.print(callback.text.toString());
            ps.close();
        }
        
        //ds.set(INSTANCES, count);
        //updateMeta (ds);
        
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "+LiverToxEntityFactory.class.getName()
                        +" DBDIR");
            System.exit(1);
        }

        try (LiverToxEntityFactory livertox =
             new LiverToxEntityFactory (argv[0])) {
            DataSource ds = livertox.register();
        }
    }
}
