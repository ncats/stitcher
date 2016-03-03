package services;

import java.io.*;
import java.util.*;
import java.net.*;
import java.util.zip.*;
import java.util.concurrent.*;

import javax.inject.Inject;
import javax.inject.Singleton;

import play.inject.ApplicationLifecycle;
import play.Logger;
import play.Configuration;
import play.libs.F;
import static play.mvc.Http.MultipartFormData.*;
import play.db.ebean.Transactional;

import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.security.SecureRandom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ix.curation.CacheFactory;
import ix.curation.Util;

import models.Payload;

@Singleton
public class CoreService {
    final public String BASE = "ix.base";
    final public String DATA = "ix.data";
    final public String HASH = "ix.hash";
    final public String WORK = "ix.work";

    final Configuration config;
    
    final File base;
    final File hash;
    final File data;
    final File work;
    CacheFactory cacheFactory;
    ExecutorService threadPool = Executors.newCachedThreadPool();

    @Inject
    public CoreService (Configuration config, ApplicationLifecycle lifecycle) {
        this.config = config;
        
        String param = config.getString(BASE);
        if (param == null) {
            throw new IllegalStateException
                ("No property \""+BASE+"\" defined in configuration file!");
        }
        
        base = new File (param);
        base.mkdirs();

        hash = new File (base, config.getString(HASH, "hash.db"));
        hash.mkdirs();
        try {
            cacheFactory = CacheFactory.getInstance(hash);
            Logger.debug("Cache "+hash+" initialized..."+cacheFactory.size());
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        data = new File (base, config.getString(DATA, "data.db"));
        data.mkdirs();

        work = new File (base, config.getString(WORK, "work"));
        work.mkdirs();

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);            
            });
    }

    public File baseDir () { return base; }
    public File hashDir () { return hash; }
    public File dataDir () { return data; }
    public File workDir () { return work; }

    protected void shutdown () {
        Logger.debug("Shutting down "+getClass()+"..."+this);
        cacheFactory.shutdown();
        threadPool.shutdown();
    }
    
    public CacheFactory getCacheFactory () { return cacheFactory; }

    public Payload upload (InputStream is) throws IOException {
        String uuid = UUID.randomUUID().toString();             
        File file = new File (work, uuid);
        FileOutputStream fos = new FileOutputStream (file);

        byte[] buf = new byte[1024];
        DigestInputStream dis = new DigestInputStream (is, Util.sha1());

        long size = 0l;
        Integer magic = null;
        for (int nb; (nb = dis.read(buf, 0, buf.length)) != -1; ) {
            if (size == 0) {
                int m = ((buf[1] & 0xff) << 8) | (buf[0] & 0xff);
                // right now we only recognize this magic
                if (m == GZIPInputStream.GZIP_MAGIC) {
                    // check for gzip ..
                    Logger.debug("*** STREAM IS GZIP COMPRESSED ***");
                    magic = m;
                }
            }

            fos.write(buf, 0, nb);
            size += nb;
        }
        dis.close();
        fos.close();

        Payload payload;
        String sha1 = Util.hex(dis.getMessageDigest().digest());
        List<Payload> results = Payload.find
            .where().eq("sha1", sha1).findList();
        if (results.isEmpty()) {
            payload = new Payload ();
            payload.timestamp = System.currentTimeMillis();
            payload.uuid = uuid;
            payload.sha1 = sha1;
            payload.size = size;
            payload.magic = magic;
        }
        else {
            payload = results.iterator().next();
            File f = new File (work, payload.uuid);
            if (f.exists()) {
                file.delete(); // delete this file..
            }
            else {
                // rename
                file.renameTo(f);
            }
        }
        
        return payload;
    }

    public Payload upload (FilePart part, Map<String, String[]> params)
        throws Exception {
        String name = part.getFilename();
        String content = part.getContentType();

        File file = part.getFile();
        Logger.debug("file="+name+" content="+content);
        
        Payload payload = upload (new FileInputStream (file));
        Logger.debug("File uploaded: uuid="
                     +payload.uuid
                     +" size="+payload.size
                     +" sha1="+payload.sha1);
        if (payload.id != null) { // already seen this file
            File f = new File (work, payload.uuid);
            f.delete();
            
            throw new RuntimeException
                ("File \""+name+"\" has already been uploaded!");
        }
        else {
            payload.title = params.get("title")[0];
            payload.comments = params.get("comments")[0];
            if (params.containsKey("uri"))
                payload.uri = params.get("uri")[0];
            payload.filename = name;
            payload.mimeType = content;
            payload.format = params.get("format")[0];
            payload.shared = params.containsKey("shared")
                ? "on".equalsIgnoreCase(params.get("shared")[0])
                : false;
            
            payload.save();
        }
        
        return payload;
    }

    public Payload upload (URI uri, Map<String, String[]> params)
        throws Exception {
        Payload payload = null;
        String scheme = uri.getScheme();
        if (scheme != null && scheme.equals("jdbc")) {
            // we'll deal with jdbc schema at a latter time
            Logger.warn("Scheme \""+scheme+"\" is currently not supported!");
            throw new RuntimeException
                ("Scheme \""+scheme+"\" currently not supported!");
        }
        else { // assume relative path for file
            URLConnection con = uri.toURL().openConnection();
            payload = upload (con.getInputStream());
            payload.title = params.get("title")[0];
            payload.comments = params.get("comments")[0];
            payload.uri = params.get("uri")[0];
            payload.mimeType = con.getContentType();
            payload.format = params.get("format")[0];
            String[] path = uri.toURL().getFile().split("/");
            if (path.length > 0)
                payload.filename = path[path.length-1];
            payload.shared = params.containsKey("shared")
                ? "on".equalsIgnoreCase(params.get("shared")[0])
                : false;

            Logger.debug("URI uploaded: uuid="
                         +payload.uuid
                         +" size="+payload.size
                         +" sha1="+payload.sha1);

            if (payload.id != null) {
                File f = new File (work, payload.uuid);
                f.delete();
                throw new RuntimeException
                    ("URI \""+uri+"\" has already been uploaded!");
            }
            payload.save();
        }
        
        return payload;
    }

    public File getFile (Payload payload) {
        File f = new File (work, payload.uuid);
        return f.exists() ? f : null;
    }

    public List<models.Payload> getPayloads () {
        return models.Payload.find.order().desc("id").findList();       
    }

    public models.Payload getPayload (String key) {
        List<models.Payload> payloads;  
        try {
            long id = Long.parseLong(key);
            payloads = models.Payload.find.where().eq("id", id).findList();
        }
        catch (NumberFormatException ex) {
            payloads = models.Payload.find
                .where().ilike("sha1", key+"%").findList();
        }
        return payloads.isEmpty() ? null : payloads.iterator().next();
    }

    public models.Payload deletePayload (String key) {
        models.Payload payload = getPayload (key);
        if (payload != null) {
            try {
                File f = getFile (payload);
                if (f != null) {
                    payload.delete();
                    f.delete();
                    Logger.debug("Successfully deleted payload "+key);
                }
                else {
                    payload = null;
                }
            }
            catch (Exception ex) {
                ex.printStackTrace();
                Logger.trace("Delete failed for payload "
                             +payload.id+"... ", ex);
                payload = null;
            }
        }
        return payload;
    }

    public List<models.Job> getJobs () {
        return models.Job.find.order().desc("id").findList();
    }
}
