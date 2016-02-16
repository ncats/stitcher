package services;

import java.io.*;
import java.util.concurrent.Callable;
import javax.inject.*;

import play.Logger;
import play.Configuration;
import play.cache.CacheApi;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import ix.curation.GraphDb;

@Singleton
public class GraphDbService {
    GraphDb graphDb;
    Service service;

    @Inject
    public GraphDbService (Service service,
                           ApplicationLifecycle lifecycle) {
        this.service = service;
        try {
            graphDb = GraphDb.getInstance(service.dataDir(),
                                          service.getCacheFactory());
        }
        catch (IOException ex) {
            ex.printStackTrace();
            Logger.error("Can't initialize graph db "+service.dataDir(), ex);
        }
        
        lifecycle.addStopHook(() -> {
                shutdown ();
                return F.Promise.pure(null);
            });
    }

    public void shutdown () {
        Logger.debug("Shutting down "+getClass()+".."+graphDb);
        graphDb.shutdown();
    }

    public GraphDb getGraphDb () { return graphDb; }
    public Service getService () { return service; }
    public CacheApi getCache () { return service.getCache(); }
}
