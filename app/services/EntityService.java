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

import ncats.stitcher.*;

@Singleton
public class EntityService {
    static public final String DATASOURCE = "IX Data Source";
        
    // unique data source associated with this instance
    //protected DataSource datasource;
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

            dsfac = new DataSourceFactory (graphDb);
            /*
            datasource = dsfac.getDataSourceByName(DATASOURCE);
            if (datasource == null) {
                // no data source ..
                datasource = dsfac.createDataSource(DATASOURCE);
                Logger.debug
                    ("Data source "+datasource
                     +" ("+datasource.getId()+") registered...");
            }
            */
            efac = new EntityFactory (graphDb);     
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

    public EntityFactory getEntityFactory () { 
        return efac;
    }
    
    public DataSourceFactory getDataSourceFactory () {
        return dsfac;
    }
    
    public long getLastUpdated () { return graphDb.getLastUpdated(); }
    public CNode getNode (long id) { return graphDb.getNode(id); }

    public DataSource register (File file) throws IOException {
        return dsfac.register(file);
    }

    public Set<DataSource> datasources () {
        return getDataSourceFactory().datasources();
    }

    public GraphMetrics calcMetrics (String label) {
        return getEntityFactory().calcGraphMetrics(label);
    }
    
    public GraphMetrics calcMetrics () {
        return getEntityFactory().calcGraphMetrics();
    }

    public Entity getEntity (long id) {
        return getEntityFactory().entity(id);
    }
    
    public Entity[] entities (String label, int skip, int top) {
        AuxNodeType type;
        if (label == null) {
            type = AuxNodeType.ENTITY;
        }
        else {
            try {
                type = AuxNodeType.valueOf(label.toUpperCase());
            }
            catch (Exception ex) {
                //ex.printStackTrace();
                //Logger.error("Unknown entity label \""+label+"\"!");
                return getEntityFactory().entities(skip, top, label);
            }
        }
        return getEntityFactory().entities(skip, top, type);
    }
}
