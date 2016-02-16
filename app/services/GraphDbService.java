package services;

import java.io.*;
import java.util.concurrent.Callable;
import javax.inject.*;

import play.Logger;
import play.Configuration;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import ix.curation.GraphDb;
import ix.curation.CacheFactory;

@Singleton
public class GraphDbService {
    static final String HOME = "ix.home";
    static final String CURATOR = "ix.curator";
    static final String CACHE = "ix.cache";

    CacheFactory cache;
    GraphDb graphDb;
    final Configuration config;

    @Inject
    public GraphDbService (Configuration config,
                           ApplicationLifecycle lifecycle) {
        this.config = config;
        String home = config.getString(HOME);
        Logger.debug(home);

        File homeDir = new File (home);
        File cacheDir = new File
            (homeDir, config.getString(CACHE, "cache.db"));
        File curatorDir = new File
            (homeDir, config.getString(CURATOR, "curator.db"));
        try {
            cacheDir.mkdirs();
            cache = CacheFactory.getInstance(cacheDir);     
            curatorDir.mkdirs();
            graphDb = GraphDb.getInstance(curatorDir, cache);
        }
        catch (IOException ex) {
            ex.printStackTrace();
            Logger.error("Can't initialize graph db "+homeDir, ex);
        }
        
        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
    }

    public void shutdown () {
        Logger.debug("Shutting down GraphDb "+graphDb);
        graphDb.shutdown();
        cache.shutdown();
    }

    public GraphDb getGraphDb () { return graphDb; }
    public CacheFactory getCache () { return cache; }
}
