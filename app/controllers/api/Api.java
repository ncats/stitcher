package controllers.api;

import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;
import java.util.concurrent.Callable;

import play.*;
import play.mvc.*;
import play.cache.*;
import play.libs.ws.*;
import static play.mvc.Http.MultipartFormData.*;
import play.db.ebean.Transactional;

import akka.actor.ActorRef;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import services.EntityService;
import services.SchedulerService;
import services.CacheService;
import services.CoreService;
import services.WebSocketConsoleActor;
import services.WebSocketEchoActor;
import services.jobs.*;

import ncats.stitcher.*;
import models.*;

public class Api extends Controller {

    class ConsoleWebSocket extends LegacyWebSocket<String> {
        final String key;
        ConsoleWebSocket (String key) {
            this.key = key;
        }

        public void onReady (WebSocket.In<String> in,
                             WebSocket.Out<String> out) {
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
    @Inject EntityService es;
    @Inject CacheService cache;
    @Inject CoreService service;
    
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
        for (DataSource ds : es.datasources()) {
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
            String[] props = (String[])ds.get(Props.PROPERTIES);
            if (props != null)
                node.put("properties", mapper.valueToTree(props));
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
                        (es.getLastUpdated(), key,new Callable<Result> () {
                                public Result call () throws Exception {
                                    Logger.debug("Cache missed: "+key);
                                    return ok ((JsonNode)mapper.valueToTree
                                               (es.calcMetrics(label)));
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

    public Result getNode (Long id) {
        try {
            CNode n = es.getNode(id);
            if (n != null) {
                return ok (n.toJson());
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return notFound ("Unknown node "+id);
    }

    public LegacyWebSocket<String> console (final String key) {
        return new ConsoleWebSocket (key);
    }

    public LegacyWebSocket<String> echo () {
        return WebSocket.withActor(WebSocketEchoActor::props);
    }

    @Transactional
    @BodyParser.Of(value = BodyParser.MultipartFormData.class)
    public Result uploader () {
        Http.MultipartFormData body = request().body().asMultipartFormData();
        FilePart part = body.getFile("file");
        if (part != null) {
            try {
                models.Payload payload = service.upload
                    (part, body.asFormUrlEncoded());
                
                return ok ((JsonNode)mapper.valueToTree(payload));
            }
            catch (RuntimeException ex) {
                return badRequest (ex.getMessage());
            }
            catch (Exception ex) {
                ex.printStackTrace();
                return internalServerError ("Can't fulfill request; "
                                            +"internal server error!");
            }
        }
        
        return noContent ();
    }

    public Result payloads () {
        List<models.Payload> payloads = service.getPayloads();  
        return ok ((JsonNode)mapper.valueToTree(payloads));
    }

    public Result jobs () {
        List<models.Job> jobs = service.getJobs();  
        return ok ((JsonNode)mapper.valueToTree(jobs));
    }

    public Result payload (String key) {
        models.Payload payload = service.getPayload(key);
        return payload != null ?
            ok ((JsonNode)mapper.valueToTree(payload))
            : notFound ("No payload "+key+" found!");
    }

    public Result download (String key) {
        models.Payload payload = service.getPayload(key);
        if (payload == null)
            return notFound ("No payload "+key+" found!");
        
        File f = service.getFile(payload);
        if (f != null) {
            response().setContentType(payload.mimeType);
            response().setHeader("Content-Disposition",
                                 "attachment; filename="+payload.filename);
            return ok (f);
        }
        
        return internalServerError ("Unable to locate payload "+key);
    }

    public Result delete (String key) {
        models.Payload payload = service.deletePayload(key);
        if (payload != null) {
            return ok ("Payload "+payload.sha1()+" deleted!");
        }
        return internalServerError ("Unable to delete payload "+key);
    }

    ObjectNode toJson (int s, int t, Entity... entities) {
        ArrayNode page = mapper.createArrayNode();
        for (Entity e : entities) {
            page.add(e.toJson());
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("skip", s);
        result.put("top", t);
        result.put("count", entities.length);
        result.put("uri", request().uri());
        result.put("contents", page);
        
        return result;
    }

    Entity getStitchEntity (Integer ver, String id) {
        Entity e = null;
        try {
            long n = Long.parseLong(id);
            e = es.getEntityFactory().entity(n);
            if (!e.is(AuxNodeType.SGROUP))
                e = null;
        }
        catch (NumberFormatException ex) {
            Entity[] entities = es.getEntityFactory()
                .filter("UNII", "'"+id+"'", "stitch_v"+ver);
            if (entities.length > 0)
                e = entities[0];
        }
        return e;
    }
    
    public Result getStitch (Integer ver, String id) {
        Entity e = getStitchEntity (ver, id);
        return e != null ? ok (e.toJson())
            : notFound ("Unknown stitch key: "+id);
    }
    
    public Result stitches (Integer ver, Integer skip, Integer top) {
        Map<String, String[]> params = request().queryString();
        if (params.isEmpty())
            return entities ("stitch_v"+ver, skip, top);

        /*
         * /stitches?filter=@label1&filter=@label2&filter=name/value
         */
        List<String> labels = new ArrayList<>();
        labels.add("stitch_v"+ver);

        String key = null, value = null;
        for (Map.Entry<String, String[]> me : params.entrySet()) {
            if ("filter".equals(me.getKey())) {
                for (String p : me.getValue()) {
                    if (p.charAt(0) == '@') { // label
                        labels.add(p.substring(1));
                    }
                    else {
                        int pos = p.indexOf('/');
                        if (pos > 0) {
                            key = p.substring(0, pos);
                            value = "'"+p.substring(pos+1)+"'";
                        }
                    }
                }
            }
        }

        Entity[] entities;
        int s, t;
        if (key != null) {
            entities = es.getEntityFactory().filter
                (key, value, labels.toArray(new String[0]));
            s = 0;
            t = entities.length;
        }
        else {
            s = skip != null ? skip : 0;
            t = top != null ? Math.min(top,1000) : 10;
            
            entities = es.getEntityFactory()
                .entities(s, t, labels.toArray(new String[0]));
        }
        
        return ok (toJson (s, t, entities));
    }
    
    public Result entities (String label, Integer skip, Integer top) {
        if ("@labels".equalsIgnoreCase(label)) {
            return ok ((JsonNode)mapper.valueToTree
                       (es.getEntityFactory().labels()));
        }
        else if ("@properties".equalsIgnoreCase(label)) {
            return ok ((JsonNode)mapper.valueToTree
                       (es.getEntityFactory().properties()));
        }
        else if ("@relationships".equalsIgnoreCase(label)) {
            return ok ((JsonNode)mapper.valueToTree
                       (es.getEntityFactory().relationships()));            
        }
        
        try {
            long id = Long.parseLong(label);
            Entity e = es.getEntity(id);
            return e != null ? ok (e.toJson())
                : notFound ("No such entity id "+id);
        }
        catch (NumberFormatException ex) {
            // not id..
        }
        
        int s = skip != null ? skip : 0;
        int t = top != null ? Math.min(top,1000) : 10;
        
        return ok (toJson (s, t, es.entities(label, s, t)));
    }
}
