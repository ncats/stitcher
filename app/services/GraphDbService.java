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
import ix.curation.EntityFactory;
import ix.curation.DataSourceFactory;

@Singleton
public class GraphDbService {
    protected GraphDb graphDb;
    protected Service service;
    protected EntityFactory efac;
    protected DataSourceFactory dsfac;

    @Inject
    public GraphDbService (Service service,
                           ApplicationLifecycle lifecycle) {
        this.service = service;
        try {
            graphDb = GraphDb.getInstance(service.dataDir(),
                                          service.getCacheFactory());
            efac = new EntityFactory (graphDb);
            dsfac = new DataSourceFactory (graphDb);
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
    public EntityFactory getEntityFactory () { return efac; }
    public DataSourceFactory getDataSourceFactory () { return dsfac; }
}
