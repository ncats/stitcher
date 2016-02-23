package services;

import java.io.File;
import java.io.IOException;
import javax.inject.Inject;
import javax.inject.Singleton;

import play.inject.ApplicationLifecycle;
import play.Logger;
import play.Configuration;
import play.libs.F;

import ix.curation.CacheFactory;

@Singleton
public class Service {
    final public String BASE = "ix.base";
    final public String DATA = "ix.data";
    final public String REPO = "ix.repo";

    final Configuration config;
    
    final File base;
    final File repo;
    final File data;
    CacheFactory cacheFactory;

    @Inject
    public Service (Configuration config, ApplicationLifecycle lifecycle) {
        this.config = config;
        
        String param = config.getString(BASE);
        if (param == null) {
            throw new IllegalStateException
                ("No property \""+BASE+"\" defined in configuration file!");
        }
        
        base = new File (param);
        base.mkdirs();

        repo = new File (base, config.getString(REPO, "repo.db"));
        repo.mkdirs();
        try {
            cacheFactory = CacheFactory.getInstance(repo);
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        data = new File (base, config.getString(DATA, "data.db"));
        data.mkdirs();

        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);            
            });
    }

    public File baseDir () { return base; }
    public File repoDir () { return repo; }
    public File dataDir () { return data; }

    public void shutdown () {
        Logger.debug("Shutting down "+getClass()+"..."+this);
        cacheFactory.shutdown();
    }
    
    public CacheFactory getCacheFactory () { return cacheFactory; }
}
