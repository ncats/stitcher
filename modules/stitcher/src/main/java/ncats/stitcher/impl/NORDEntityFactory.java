package ncats.stitcher.impl;

import java.io.*;
import java.util.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Logger;
import java.util.logging.Level;

import javax.swing.text.MutableAttributeSet;
import javax.swing.text.html.*;
import javax.swing.text.html.parser.*;

import ncats.stitcher.*;
import static ncats.stitcher.StitchKey.*;

/*
 * NORD
 */
public class NORDEntityFactory extends EntityRegistry {
    static final String NORD_URL =
        "https://rarediseases.org/for-patients-and-families/information-resources/rare-disease-information/page/";
    static final String USER_AGENT =
	"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.95 Safari/537.11";
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
    
    static class NORDPagingCallback extends Callback {
        boolean article;
        String url;
        Map<String, String> pages = new TreeMap<>();
        
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

            if (endtag) {
                HTML.Tag t = stack.pop();
                if ("article".equals(t.toString()))
                    article = false;
            }
            else {
                if ("article".equals(tag.toString())) {
                    article = true;
                    url = null;
                }
                stack.push(tag);

                if (article) {
                    if ("a".equals(tag.toString()))
                        url = params.get("href");
                    /*
                    StringBuilder indent = new StringBuilder ();
                    for (int i = 0; i < stack.size(); ++i) indent.append(" ");
                    logger.info(indent.toString()+tag+": "+params);
                    */
                }
            }
        }

        @Override
        public void handleText (char[] chrs, int pos) {
            if (article) {
                String text = new String (chrs);
                if (url != null) {
                    pages.put(text, url);
                }
            }
        }
    }

    static class NORDDiseaseCallback extends Callback {
        String id;
        List<String> synonyms = new ArrayList<>();
        Map<String, StringBuilder> texts = new LinkedHashMap<>();
        
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
                if ("rdr-box".equals(params.get("class")))
                    id = params.get("id");
                break;
            }
            
            if (endtag) {
                HTML.Tag t = stack.pop();
            }
            else {
                /*
                StringBuilder indent = new StringBuilder ();
                for (int i = 0; i < stack.size(); ++i) indent.append(" ");
                logger.info(indent.toString()+tag+": "+params);
                */
                stack.push(tag);
            }            
        }
        
        @Override
        public void handleText (char[] chrs, int pos) {
            String tag = null;
            if (!stack.isEmpty())
                tag = stack.peek().toString();
            
            String text = new String (chrs);
            if ("synonyms".equals(id)) {
                if ("li".equals(tag)) {
                    synonyms.add(text);
                }
            }
            else if ("general-discussion".equals(id)
                     || "symptoms".equals(id)
                     || "causes".equals(id)
                     || "affected-populations".equals(id)
                     || "related-disorders".equals(id)
                     || "diagnosis".equals(id)
                     || "standard-therapies".equals(id)
                     || "investigational-therapies".equals(id)) {
                if ("h4".equals(tag) || "strong".equals(tag)) {
                }
                else {
                    StringBuilder sb = texts.get(id);
                    if (sb == null)
                        texts.put(id, sb = new StringBuilder ());
                    if (sb.length() > 0 && sb.charAt(sb.length()-1) != ' '
                        && text.charAt(0) != ' ')
                        sb.append(' ');
                    sb.append(text);
                }
            }
            //logger.info("text: "+text);
        }
    }

    public NORDEntityFactory (String dir) throws IOException {
        super (dir);
    }

    public NORDEntityFactory (File dir) throws IOException {
        super (dir);
    }

    public NORDEntityFactory (GraphDb graphDb) throws IOException {
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
        DataSource ds = getDataSourceFactory().register("NORD");
        Integer size = (Integer)ds.get(INSTANCES);
        if (size != null && size > 0) {
            logger.info(ds.getName()+" ("+size
                        +") has already been registered!");
            return ds;
        }
        setDataSource (ds);
        
        DocumentParser parser = new DocumentParser (DTD.getDTD(""));
        Map<String, String> pages = new TreeMap<>();
        int page = 1, count = 0;
        do {
            URL url = new URL(NORD_URL+page);
            logger.info("#### processing "+url);
	    URLConnection con = url.openConnection();
	    con.setRequestProperty("User-Agent", USER_AGENT);
            
            NORDPagingCallback nord = new NORDPagingCallback ();
            parser.parse(new InputStreamReader (con.getInputStream()),
			 nord, true);
            if (nord.pages.isEmpty())
                break;
            
            for (Map.Entry<String, String> me : nord.pages.entrySet()) {
                Map<String, Object> disease = new TreeMap<>();
                disease.put("name", me.getKey());
                disease.put("url", me.getValue());
                NORDDiseaseCallback cb = new NORDDiseaseCallback ();
		URLConnection con2 = new URL(me.getValue()).openConnection();
		con2.setRequestProperty("User-Agent", USER_AGENT);
                parser.parse(new InputStreamReader
                             (con2.getInputStream()), cb, true);
                disease.put("synonyms", cb.synonyms.toArray(new String[0]));
                for (Map.Entry<String, StringBuilder> e : cb.texts.entrySet()) {
                    disease.put(e.getKey(), e.getValue().toString());
                }

                ncats.stitcher.Entity e = register (disease);
                ++count;
                logger.info("+++++++++ "+count+": "+me.getKey()+" "+e.getId());
            }
            ++page;
        }
        while (true);

        logger.info(count+" entities registered!");
        ds.set(INSTANCES, count);
        updateMeta (ds);
        
        return ds;
    }

    public static void main (String[] argv) throws Exception {
        if (argv.length < 1) {
            logger.info("Usage: "+NORDEntityFactory.class.getName()
                        +" DBDIR");
            System.exit(1);
        }

        try (NORDEntityFactory nord = new NORDEntityFactory (argv[0])) {
            DataSource ds = nord.register();
        }
    }
}
