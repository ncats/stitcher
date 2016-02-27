package services;

import java.io.InputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.UUID;
import java.util.Properties;

import javax.inject.Inject;
import javax.inject.Singleton;

import play.inject.ApplicationLifecycle;
import play.Logger;
import play.Configuration;
import play.libs.F;

import java.security.MessageDigest;
import java.security.DigestInputStream;
import java.security.SecureRandom;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import ix.curation.CacheFactory;
import ix.curation.Util;


@Singleton
public class CoreService {
    public class Payload {
        public JsonNode props;
        public File file;
    }
    
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

    public void shutdown () {
        Logger.debug("Shutting down "+getClass()+"..."+this);
        cacheFactory.shutdown();
    }
    
    public CacheFactory getCacheFactory () { return cacheFactory; }

    public Payload upload (InputStream is, ObjectNode props)
        throws IOException {
        String uuid = UUID.randomUUID().toString();

        Payload payload = new Payload ();
        payload.file = new File (work, uuid);

        FileOutputStream fos = new FileOutputStream (payload.file);
        byte[] buf = new byte[1024];
        DigestInputStream dis = new DigestInputStream (is, Util.sha1());
        long size = 0l;
        for (int nb; (nb = dis.read(buf, 0, buf.length)) != -1; ) {
            fos.write(buf, 0, nb);
            size += nb;
        }
        dis.close();
        fos.close();
        
        String sha1 = Util.hex(dis.getMessageDigest().digest());
        payload.props = props;
        props.put("uuid", uuid);
        props.put("size", size);
        props.put("sha1", sha1);

        ObjectMapper mapper = new ObjectMapper ();
        mapper.writeValue(new File (work, uuid+".json"), props);
        
        return payload;
    }
}
