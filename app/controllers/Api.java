package controllers;

import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;
import java.util.concurrent.Callable;

import play.*;
import play.mvc.*;
import play.cache.*;
import play.libs.ws.*;

import akka.actor.ActorRef;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import services.GraphDbService;
import services.SchedulerService;
import services.CacheService;
import services.WebSocketConsoleActor;
import services.WebSocketEchoActor;
import services.jobs.*;
import utils.JsonUtil;

import ix.curation.*;

public class Api extends Controller {

    class ConsoleWebSocket extends WebSocket<String> {
        final String key;
        ConsoleWebSocket (String key) {
            this.key = key;
        }

        public void onReady (In<String> in, Out<String> out) {
        }
        
        public boolean isActor () { return true; }
        public akka.actor.Props actorProps (ActorRef out) {
            try {
                return akka.actor.Props.create
                    (WebSocketConsoleActor.class, out, key, cache);
            }
            catch (Exception ex) {
                throw new RuntimeException (ex);
            }
        }
    }
    
    @Inject SchedulerService scheduler;
    @Inject play.Application app;
    @Inject GraphDbService graphDb;
    @Inject CacheService cache;
    
    ObjectMapper mapper = new ObjectMapper ();
    
    public Api () {
    }

    public Result getRunningJobs () {
        try {
            List<Map> jobs = scheduler.getRunningJobs();
            return ok ((JsonNode)mapper.valueToTree(jobs));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result getDataSources () {
        ArrayNode sources = mapper.createArrayNode();
        for (DataSource ds : graphDb.getDataSourceFactory().datasources()) {
            ObjectNode node = mapper.createObjectNode();
            node.put("key", ds.getKey());
            node.put("name", ds.getName());
            /*
            if (null != ds.toURI()) {
                node.put("uri", ds.toURI().toString());
            }
            */
            node.put("created", (Long)ds.get(Props.CREATED));
            node.put("count", (Integer)ds.get(Props.INSTANCES));
            node.put("sha1", (String)ds.get(Props.SHA1));
            node.put("size", (Long)ds.get(Props.SIZE));
            String[] stitches = (String[])ds.get(Props.STITCHES);
            if (stitches != null) {
                ObjectNode sn = mapper.createObjectNode();
                for (String s : stitches) {
                    String[] props = (String[])ds.get(s);
                    if (props != null)
                        sn.put(s, JsonUtil.toJsonNode(props));
                }
                node.put("stitches", sn);
            }
            String[] props = (String[])ds.get(Props.PROPERTIES);
            if (props != null)
                node.put("properties", JsonUtil.toJsonNode(props));
            sources.add(node);
        }
        return ok (sources);
    }

    public Result getMetrics (final String label) {
        Object metrics = cache.get(JobParams.METRICS);
        if (metrics != null) {
            if (label != null) {
                try {
                    final String key = routes.Api.getMetrics(label).toString();
                    return cache.getOrElse
                        (graphDb.getLastUpdated(), key,new Callable<Result> () {
                                public Result call () throws Exception {
                                    Logger.debug("Cache missed: "+key);
                                    return ok ((JsonNode)mapper.valueToTree
                                               (graphDb.getEntityFactory()
                                                .calcCurationMetrics(label)));
                                }
                            });
                }
                catch (Exception ex) {
                    return internalServerError (ex.getMessage());
                }
            }
            return ok ((JsonNode)mapper.valueToTree(metrics));
        }
        
        return notFound ("Metrics are not yet available!");
    }

    public Result getResult (String id) {
        Object result = cache.get(id);
        if (result != null) {
            return ok ((JsonNode)mapper.valueToTree(result));
        }
        return notFound ("Unknown result key "+id);
    }

    public WebSocket<String> console (final String key) {
        return new ConsoleWebSocket (key);
    }

    public WebSocket<String> echo () {
        return WebSocket.withActor(WebSocketEchoActor::props);
    }
}
