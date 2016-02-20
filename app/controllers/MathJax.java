package controllers;

import java.io.*;
import java.security.SecureRandom;
import java.util.concurrent.Callable;
import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;

import views.html.*;
import ix.curation.CacheFactory;

public class MathJax extends Controller {
    final SecureRandom rand = new SecureRandom ();
    CacheFactory cache;
    File workDir;
    final Configuration config;

    @Inject
    public MathJax (Configuration config) {
        this.config = config;
        try {
            cache = CacheFactory.getInstance
                (config.getString("ix.cache", "cache.db"));
            workDir = new File (config.getString("ix.work", "work"));
            workDir.mkdirs();
        }
        catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    String getText (final String id) {
        try {
            return cache.getOrElse(id, new Callable<String> () {
                    public String call () throws Exception {
                        File file = new File (workDir, id);
                        if (!file.exists())
                            return null;

                        StringBuilder buf = new StringBuilder ();
                        BufferedReader br = new BufferedReader
                            (new FileReader (file));
                        for (String line; (line = br.readLine()) != null;) {
                            buf.append(line+"\n");
                        }
                        return buf.toString();
                    }
                });
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public Result render (String id) {
        if (id != null) {
            String text = getText (id);
            if (text != null) {
                return ok (mathjax.render(text));
            }
        }
        return ok (mathbox.render());
    }
    
    @BodyParser.Of(value=BodyParser.FormUrlEncoded.class, maxLength=1024*10)
    public Result mathjax () {
        String[] params = request().body().asFormUrlEncoded().get("matharea");
        if (params != null && params.length > 0) {
            String text = params[0];
            byte[] b = new byte[4];
            rand.nextBytes(b);
            
            StringBuilder buf = new StringBuilder ();
            for (int i = 0; i < b.length; ++i) {
                buf.append(String.format("%1$02x", b[i] & 0xff));
            }
            String id = buf.toString();
            try {
                File out = new File (workDir, id);
                PrintStream ps = new PrintStream (new FileOutputStream (out));
                ps.print(text);
                ps.close();
                
                Logger.debug("Rending text... "+id+"\n"+text);

                return redirect (routes.MathJax.render(id));
            }
            catch (IOException ex) {
                ex.printStackTrace();
            }
        }
        
        return redirect (routes.MathJax.render(null));
    }
}
