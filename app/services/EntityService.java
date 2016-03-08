package services;

import java.io.*;
import java.util.*;
import java.util.concurrent.Callable;
import javax.inject.*;

import play.Logger;
import play.Configuration;
import play.cache.CacheApi;
import play.inject.ApplicationLifecycle;
import play.libs.F;

import ix.curation.*;

@Singleton
public class EntityService {
    static public final String DATASOURCE = "IX Data Source";
        
    // unique data source associated with this instance
    protected DataSource datasource;
    
    protected GraphDb graphDb;
    protected CoreService service;
    protected EntityFactory efac;
    protected DataSourceFactory dsfac;

    @Inject
    public EntityService (CoreService service,
                          ApplicationLifecycle lifecycle) {
        this.service = service;
        try {
            graphDb = GraphDb.getInstance(service.dataDir(),
                                          service.getCacheFactory());
            efac = new EntityFactory (graphDb);
            dsfac = new DataSourceFactory (graphDb);
            datasource = dsfac.getDataSourceByName(DATASOURCE);
            if (datasource == null) {
                // no data source ..
                datasource = dsfac.createDataSource(DATASOURCE);
                Logger.debug
                    ("Data source "+datasource
                     +" ("+datasource.getId()+") registered...");
            }
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

    protected void shutdown () {
        Logger.debug("Shutting down "+getClass()+".."+graphDb);
        graphDb.shutdown();
    }

    public GraphDb getGraphDb () { return graphDb; }
    public EntityFactory getEntityFactory () { return efac; }
    public DataSourceFactory getDataSourceFactory () { return dsfac; }
    public long getLastUpdated () { return graphDb.getLastUpdated(); }
    public CNode getNode (long id) { return graphDb.getNode(id); }

    public DataSource register (File file) throws IOException {
        return dsfac.register(file);
    }

    public Set<DataSource> datasources () {
        return dsfac.datasources();
    }

    public GraphMetrics calcMetrics (String label) {
        return efac.calcGraphMetrics(label);
    }
    public GraphMetrics calcMetrics () {
        return efac.calcGraphMetrics();
    }
}
