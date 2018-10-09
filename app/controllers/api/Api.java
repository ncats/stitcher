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
import play.libs.streams.ActorFlow;
import akka.actor.*;
import akka.stream.*;
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
import ncats.stitcher.calculators.CalculatorFactory;
import serializer.JsonCodec;

import models.*;
import controllers.Util;
import chemaxon.struc.Molecule;

public class Api extends Controller {
    
    @Inject SchedulerService scheduler;
    @Inject EntityService es;
    @Inject CacheService cache;
    @Inject CoreService service;
    @Inject ActorSystem actorSystem;
    @Inject Materializer materializer;
    @Inject JsonCodec jsonCodec;
    
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
            URI uri = ds.toURI();
            node.put("source", uri == null ?
                    null :
                    uri.toString().indexOf('/') > -1 ?
                            uri.toString() :
                            uri.toString().substring(uri.toString().lastIndexOf('/')+1));
            /*
            if (null != ds.toURI()) {
                node.put("uri", ds.toURI().toString());
            }
            */
            node.put("created", (Long)ds.get(ncats.stitcher.Props.CREATED));
            node.put("count", (Integer)ds.get(ncats.stitcher.Props.INSTANCES));
            node.put("sha1", (String)ds.get(ncats.stitcher.Props.SHA1));
            node.put("size", (Long)ds.get(ncats.stitcher.Props.SIZE));
            String[] props = (String[])ds.get(ncats.stitcher.Props.PROPERTIES);
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
                return ok (jsonCodec.encode(n));
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        return notFound ("Unknown node "+id);
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
            page.add(jsonCodec.encode(e));
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
        catch (Exception ex) {
            Entity[] entities = es.getEntityFactory()
                    .filter("id", "'"+id+"'", "stitch_v"+ver);
            if (entities.length > 0) {
                int index = 0;
                if (entities.length > 1) {
                    Logger.warn(id + " yields " + entities.length
                            + " matches!");
                    int highestrank = 0; // make which stitchnode is returned to be more deterministic
                    for (int i = 0; i < entities.length; i++) {
                        Entity ent = entities[i];
                        Map props = ent.properties();
                        int rank = 0;
                        if (props.containsKey("rank"))
                            rank = (Integer) props.get("rank");
                        if (rank > highestrank) {
                            highestrank = rank;
                            index = i;
                        } else if (rank == highestrank && ent.getId() < entities[index].getId()) {
                            index = i;
                        }
                    }
                }
                e = entities[index];
            }
        }
        return e;
    }

    public Result getStitches (Integer ver, String id, String format) {
        String uri = routes.Api.getStitches(ver, id, format).url();
        Logger.debug(uri);

        JsonNode json = mapper.createArrayNode();
        try {
            long n = Long.parseLong(id);
            Entity e = es.getEntityFactory().entity(n);
            if (e.is(AuxNodeType.SGROUP))
                json = jsonCodec.encodeSimple(e);
        }
        catch (Exception ex) {
            Entity[] entities = es.getEntityFactory()
                .filter("id", "'"+id+"'", "stitch_v"+ver);
            for (int i = 0; i < entities.length; ++i) {
                JsonNode n = jsonCodec.encode(entities[i]);
                if ("simple".equals(format))
                    n = jsonCodec.encodeSimple(entities[i]);
                if (n.isArray()) {
                    // unwrap this array
                    ArrayNode an = (ArrayNode) n;
                    for (int j = 0; j < an.size(); ++j) {
                        if ("simple".equals(format))
                            ((ObjectNode) an.get(j)).put("node", entities[i].getId());
                        ((ArrayNode) json).add(an.get(j));
                    }
                } else {
                    if ("simple".equals(format))
                        ((ObjectNode) n).put("node", entities[i].getId());
                    ((ArrayNode) json).add(n);
                }
            }
        }

        ObjectNode node = mapper.createObjectNode();
        node.put("uri", uri);
        node.put("count", json.size());
        node.put("data", json);

        return ok (node);
    }

    public Result getLatestStitch (String id) {
        String uri = routes.Api.getLatestStitch(id).url();
        Logger.debug(uri);
        
        Integer ver = service.getLatestVersion();
        if (null == ver)
            return badRequest ("No latest stitch version defined!");
        return getStitch (ver, id);
    }

    public Result getLatestStitches (String id, String format) {
        String uri = routes.Api.getLatestStitches(id, format).url();
        Logger.debug(uri);

        Integer ver = service.getLatestVersion();
        if (null == ver)
            return badRequest ("No latest stitch version defined!");
        return getStitches (ver, id, format);
    }

    public Result getStitch (Integer ver, String id) {
        String uri = routes.Api.getStitch(ver, id).url();
        Logger.debug(uri);
        
        Entity e = getStitchEntity (ver, id);
        return e != null ? ok (jsonCodec.encode(e))
            : notFound ("Unknown stitch key: "+id);
    }

    public Result getComponent (Long id) {
        String uri = routes.Api.getComponent(id).url();
        Logger.debug(uri);
        
        try {
            Component comp = es.getEntityFactory().component(id);
            ArrayNode json = mapper.createArrayNode();
            for (Entity e : comp) {
                ArrayNode a = (ArrayNode)jsonCodec.encodeSimple(e);
                for (int i = 0; i < a.size(); ++i)
                    json.add(a.get(i));
            }

            ObjectNode node = mapper.createObjectNode();
            node.put("uri", uri);
            node.put("count", json.size());
            node.put("data", json);
            
            return ok (node);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            return notFound ("Unknown component: "+id);
        }
    }

    public Result updateStitch(Integer ver, String id) {
        String uri = routes.Api.updateStitch(ver, id).url();
        Logger.debug(uri);
        
        Entity e = getStitchEntity(ver, id);

        if (e != null) {
            CalculatorFactory.getCalculatorFactory(es.getEntityFactory())
                             .process(Stitch.getStitch(e));
            return getStitch(ver, id);
        } else {
            return notFound("Unknown stitch key: " + id);
        }

    }

    public Result latestStitches (Integer skip, Integer top) {
        Integer ver = service.getLatestVersion();
        if (ver == null)
            return badRequest ("No latest version defined!");

        return stitches (ver, skip, top);
    }
    
    public Result stitches (Integer ver, Integer skip, Integer top) {
        String uri = routes.Api.stitches(ver, skip, top).url();
        Logger.debug(uri);
        
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
            t = top != null ? Math.min(top,1000) : 5;

            String page = request().getQueryString("page");
            if (page != null) {
                try {
                    skip = (Integer.parseInt(page)-1)*t;
                    s = Math.max(0, skip);
                }
                catch (NumberFormatException ex) {
                    Logger.error("Bogus page number: "+page, ex);
                }
            }
            
            entities = es.getEntityFactory()
                .entities(s, t, labels.toArray(new String[0]));
        }
        
        return ok (toJson (s, t, entities));
    }
    
    public Result entities (String label, Integer skip, Integer top) {
        String uri = routes.Api.entities(label, skip, top).url();
        Logger.debug(uri);
        
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
            return e != null ? ok (jsonCodec.encode(e))
                : notFound ("No such entity id "+id);
        }
        catch (NumberFormatException ex) {
            // not id..
        }
        
        int s = skip != null ? skip : 0;
        int t = top != null ? Math.min(top,1000) : 10;
        
        return ok (toJson (s, t, es.entities(label, s, t)));
    }

    public Result structure (Long id, String format, Integer size) {
        String uri = routes.Api.structure(id, format, size).url();
        Logger.debug(uri);

        Entity e = es.getEntityFactory().entity(id);
        if (e != null) {
            Molecule mol = e.mol();
            
            if (mol == null && e.is(AuxNodeType.SGROUP))
                mol = Stitch.getStitch(e).mol();

            if (mol != null) {
                switch (format) {
                case "svg":
                    try {
                        return ok (Util.renderMol(mol, format, size, null))
                            .as("image/svg+xml");
                    }
                    catch (Exception ex) {
                        return internalServerError
                            ("Can't generate structure format "+format);
                    }
                    
                case "png":
                    try {
                        return ok (Util.renderMol(mol, format, size, null))
                            .as("image/png");
                    }
                    catch (Exception ex) {
                        return internalServerError
                            ("Can't generate structure format "+format);
                    }
                    
                case "mol":
                case "sdf":
                case "smi":
                case "smiles":
                case "mrv":
                    return ok (mol.toFormat(format));
                    
                default:
                    return badRequest (uri+": Unknown format: "+format);
                }
            }
            else
                return badRequest (uri+": Entity "+id+" has no structure!");
        }
        
        return notFound (uri+": Unknown entity: "+id);
    }
}
