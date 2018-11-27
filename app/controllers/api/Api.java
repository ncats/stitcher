package controllers.api;

import java.lang.reflect.Array;
import java.net.URLDecoder;
import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;
import java.util.concurrent.Callable;

import ncats.stitcher.Props;
import ncats.stitcher.tools.CompoundStitcher;
import org.h2.mvstore.DataUtils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
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
            node.put("_idField", (String)ds.get("IdField"));
            node.put("_NameField", (String)ds.get("NameField"));
            node.put("_StructField", (String)ds.get("StrucField"));

            String[] props = (String[])ds.get(ncats.stitcher.Props.PROPERTIES);
            if (props != null)
                node.put("properties", mapper.valueToTree(props));
            String[] stitches = (String[])ds.get(ncats.stitcher.Props.STITCHES);
            if (stitches != null) {
                node.put("stitches", mapper.valueToTree(stitches));
                for (String stitch: stitches)
                    node.put("_stitch_"+stitch, mapper.valueToTree(ds.get("_stitch_"+stitch)));
            }
            String[] refs = (String[])ds.get(ncats.stitcher.Props.REFERENCES);
            if (refs != null && refs.length > 0) {
                node.put("references", mapper.valueToTree(refs));
                for (String ref: refs) {
                    node.put("_ref_" + ref, mapper.valueToTree(ds.get("_ref_" + ref)));
                    node.put("_refName_" + ref, mapper.valueToTree(ds.get("_refName_" + ref)));
                }
            }
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
            e = es.getEntityFactory().entity(ver, id);
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

    public Result getUsApprovalRx (String id, String format) {
        String uri = routes.Api.getLatestStitches(id, format).url();
        Logger.debug(uri);

        Integer ver = service.getLatestVersion();
        if (null == ver)
            return badRequest ("No latest stitch version defined!");

        JsonNode json = mapper.createArrayNode();
        Entity[] entities = es.getEntityFactory().entities( 0, 10000, "USApprovalRx");
        for (Entity e: entities) {
            JsonNode ent = jsonCodec.encode(e);
            String[] item = new String[24];
            String eventID = (String)e.get("initiallyMarketedUS");
            for (JsonNode event: ent.get("events")) {
                if (event.get("id").asText().equals(eventID)) {
                    item[14] = event.get("startDate").asText();
                }
            }
            //1+ first   first-in-class
            //2+ orphan
            //3+ fastTrack
            //4+ breakthrough
            //5+ priority
            //6+ accelerated
            //7+ initClinicalStudy
            //8+ nctDate
            //9+ therapeuticClass
            //10+ substanceClass
            //11 moleculeType
            //12+ name
            //13+ ingredients
            //14+ dateString
            //15 use
            //16+ disease
            //17 modeOfAction
            //18+ innovation
            //19 bla    NDA or other application number
            //20 press   URL to FDA website for press release
            //21 trials   URL to FDA website for trials press release
            //22+ unii
            //23 pharmacology
            //24+ target
            String entry = Arrays.toString(item);
            if ("9842X06Q6M".equals(eventID))
                ((ArrayNode) json).add(entry);
        }

        ObjectNode node = mapper.createObjectNode();
        node.put("uri", uri);
        node.put("count", json.size());
        node.put("data", json);

        return ok (node);
    }

    public Result updateEvents(Integer ver, String id) {
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

    public Result updateLatestEvents (String id) {
        String uri = routes.Api.getLatestStitch(id).url();
        Logger.debug(uri);

        Integer ver = service.getLatestVersion();
        if (null == ver)
            return badRequest ("No latest stitch version defined!");
        return updateEvents (ver, id);
    }

    Object editProperty(Object value, String newVal, String oldVal) throws Exception {
        if (value.getClass().isArray()) {
            boolean found = false;
            ArrayList<String> vals = new ArrayList();
            for (int i = 0; i < Array.getLength(value); ++i) {
                String v = Array.get(value, i).toString();
                if (v.equals(oldVal)) { // remove or replace
                    found = true;
                    if (newVal != null) // replace
                        vals.add(newVal);
                } else {
                    vals.add(v);
                }
            }
            if (oldVal == null) // add
                vals.add(newVal);
            else if (!found) {
                throw new Exception("Value to be replaced not found! "+oldVal+": "+newVal);
            }
            value = vals.toArray(new String[0]);
        } else {
            if (oldVal == null) {
                throw new Exception("Can't add new value to property - property value already exists: "+value.toString());
            }
            value = newVal;
        }
        return value;
    }

    @Transactional
    @BodyParser.Of(value = BodyParser.TolerantJson.class)
    Result updateStitch(Integer ver, String id, boolean test) {
        ObjectNode message = mapper.createObjectNode();
        message.put("status", "ok");
        ObjectNode update = (ObjectNode)request().body().asJson();
        message.put("request", update);
        if (update != null && update.has("jsonPath") &&
                update.get("jsonPath").asText().startsWith("$['properties'][?(@['key']==")) {

            //"jsonPath": "$['properties'][?(@['key']=='CompoundUNII' )]['value']",
            String updateProperty = update.get("jsonPath").asText();
            updateProperty = updateProperty.substring(29,updateProperty.indexOf("' )]['value']"));

            Entity stitchNode = getStitchEntity(ver, id);
            if (stitchNode != null) {
                Entity updateNode = null;
                // TODO fix these shenanigans to get component more directly
                JsonNode node = jsonCodec.encode(stitchNode);
                long stitchId = node.get("id").asLong();
                long rootId = node.get("sgroup").get("parent").asLong();
                Entity root = es.getEntity(rootId);
                ArrayList<Long> nodes = new ArrayList();
                //long[] nodes = new long[node.get("sgroup").get("size").asInt()];
                int index = 0;
                for (JsonNode entry: node.get("sgroup").get("members")) {
                    Long nodeId = entry.get("node").asLong();
                    nodes.add(nodeId);
                    Entity item = es.getEntity(nodeId);
                    if (item.datasource().getName().equals(update.get("nodeSource").asText()) &&
                        entry.get("id").asText().equals(update.get("nodeId").asText())) {
                        if (Long.valueOf(update.get("node").asText()) != nodeId) {
                            Logger.warn("node ID has changed since original curation! "+item.name()+" "+update.get("node").asText());
                            update.set("nodeId", entry.get("id"));
                        }
                        updateNode = es.getEntity(nodeId);
                    }
                    index++;
                }

                if (updateNode != null) {
                    try {
                        // remove auto-generated data by previous instance of database
                        ArrayList<String> remove = new ArrayList();
                        for (Iterator<String> i=update.fieldNames(); i.hasNext();) {
                            String field = i.next();
                            if (field.startsWith("_"))
                                remove.add(field);
                        }
                        for (String field: remove)
                            update.remove(field);

                        String response = "nothing happened; payload would have been updated";

                        String oldVal = update.has("oldValue") ? update.get("oldValue").asText() : null;
                        String newVal = update.has("value") ? update.get("value").asText() : null;
                        String operation = update.has("operation") ? update.get("operation").asText() : null;
                        if ("replace".equals(operation) && (oldVal == null || newVal == null)) {
                            throw new Exception("Can't replace if old or new value is null");
                        } else if ("remove".equals(operation) && newVal != null) {
                            throw new Exception("New value must be null if removing");
                        } else if (("replace".equals(operation) || "remove".equals(operation)) &&
                                !updateNode.payload().containsKey(updateProperty)) {
                            throw new Exception("Payload does not contain property: "+updateProperty);
                        } else if ("add".equals(operation) && oldVal != null) {
                            throw new Exception("Old value must be null if adding anew");
                        }

                        // create new payload object
                        DefaultPayload payload = new DefaultPayload (updateNode.datasource());
                        payload.setId(updateNode.payload().get("id"));
                        for (Map.Entry<String, Object> me: updateNode.payload().entrySet()) {
                            Object value = me.getValue();
                            if (me.getKey().equals(updateProperty)) {
                                value = editProperty(me.getValue(), newVal, oldVal);
                            }
                            if (value != null)
                                payload.put(me.getKey(), value);
                        }
                        if ("add".equals(operation) && !updateNode.payload().containsKey(updateProperty)) {
                            payload.put(updateProperty, newVal);
                        }

                        // add the curation itself to the payload
                        String updateStr = update.toString();
                        updateStr = updateStr.substring(0, updateStr.length()-1) +
                                ",\"_ver\":\""+ver+"\",\"_stitch\":\""+id+"\",\"_uri\":\""+request().uri()+
                                "\",\"_timestamp\":"+new Date().getTime()+"}";
                        Object value = updateNode.payload("_CURATION");
                        if (value == null) {
                            String[] upSA = new String[1];
                            upSA[0] = updateStr;
                            payload.put("_CURATION", upSA);
                        } else {
                            payload.put("_CURATION", editProperty(value, updateStr, null));
                        }

                        // see if it's a stitchkey
                        DataSource ds = updateNode.datasource();
                        StitchKey sk = null;
                        String[] stitches = (String[])ds.get(ncats.stitcher.Props.STITCHES);
                        if (stitches != null) {
                            for (String stitch: stitches) {
                                for (String sprop: (String[])ds.get("_stitch_"+stitch)) {
                                    if (sprop.equals(updateProperty)) {
                                        sk = StitchKey.valueOf(stitch);
                                    }
                                }
                            }
                        }

                        // TODO see if it affects a referenced node --- update node is wrong, then?
                        // for referenced node, node becomes a payload on that referenced node
//                        String[] refs = (String[])ds.get(ncats.stitcher.Props.REFERENCES);
//                        if (refs != null && refs.length > 0) {
//                            node.put("references", mapper.valueToTree(refs));
//                            for (String ref: refs) {
//                                node.put("_ref_" + ref, mapper.valueToTree(ds.get("_ref_" + ref)));
//                                node.put("_refName_" + ref, mapper.valueToTree(ds.get("_refName_" + ref)));
//                            }
//                        }

                        if (!test) {
                            updateNode.add(payload);
                            updateNode.addLabel(Props.CURATED);
                            response = "payload updated";
                        }

                        if (sk != null) { // check if stitching needs to be redone
                            List<Stitch> sL = new ArrayList();
                            sL.add(updateNode.getStitch(ver));

                            updateNode.update(sk, oldVal, newVal);
                            for (Entity e : updateNode.neighbors(sk, newVal)) {
                                Stitch s = e.getStitch(ver);
                                if (s != null && !sL.contains(s)) {
                                    sL.add(s);
                                    for (Map m : s.members()) {
                                        Long member = Long.valueOf(m.get("parent").toString());
                                        if (!nodes.contains(member))
                                            nodes.add(member);
                                    }
                                }
                            }
                            long[] cn = new long[nodes.size()];
                            for (int i = 0; i < nodes.size(); i++)
                                cn[i] = nodes.get(i);
                            CompoundStitcher cs = new CompoundStitcher(es.getEntityFactory());
                            Component comp = es.getEntityFactory().component(rootId, cn);
                            List<String> nSL = cs.testStitch(ver, comp);
                            if (test) {
                                // undo stitchkey update if just testing
                                updateNode.update(sk, newVal, oldVal);
                                response = "updating payload and stitchkey, does not affect stitching.";
                                if (nSL.size() != sL.size()) {
                                    response = "updating payload and stitchkey causes restitching of node.";
                                    ArrayNode nSLA = mapper.createArrayNode();
                                    for (String entry: nSL)
                                        nSLA.add(entry);
                                    message.put("newStitches", nSLA);
                                    message.put("previousStitchCount", sL.size());
                                }
                            } else if (nSL.size() != sL.size()) {
                                ArrayNode nSLA = mapper.createArrayNode();
                                for (String entry: nSL)
                                    nSLA.add(entry);
                                message.put("newStitches", nSLA);
                                message.put("previousStitchCount", sL.size());
                                for (Stitch s : sL)
                                    s.delete();
                                sL = cs.stitch(ver, comp);
                                response = "updated payload, stitchkey and restitched node.";
                                for (Stitch s : sL)
                                    for (Map me : s.members())
                                        if (me.containsKey("parent") && me.get("parent").equals(updateNode.getId())) {
                                            message.put("newStitch", jsonCodec.encode(s));
                                        }
                            } else {
                                response = "updated payload and stitchkey, but stitching not affected.";
                            }
                        }
                        message.put("status", response);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        message.put("status", "Error stitching: " + root.getId() + "\n" + ex.getMessage());
                    }
                } else {
                    message.put("status", "Can't find updateNode on stitch key: " + id + ":" + update.get("nodeSource").asText() + ":" + update.get("nodeId").asText());
                }
            } else {
                message.put("status", "Unknown stitch key: " + id);
            }

        } else {
            message.put("status", "update does not contain \"jsonPath\"");
            if (update == null)
                message.put("status", "Update json object is missing, recalculating events");
        }

        return ok(message);
    }

    public Result updateStitch(Integer ver, String id) {
        return updateStitch (ver, id, false);
    }

    public Result testUpdateStitch(Integer ver, String id) {
        return updateStitch (ver, id, true);
    }

    Result updateLatestStitch (String id, boolean test) {
        String uri = routes.Api.getLatestStitch(id).url();
        Logger.debug(uri);

        Integer ver = service.getLatestVersion();
        if (null == ver)
            return badRequest ("No latest stitch version defined!");
        return updateStitch (ver, id, test);
    }

    public Result dumpCuratedNodes (final String label, Integer skip, Integer top) {
        String uri = routes.Api.dumpCuratedNodes(label, skip, top).url();
        Logger.debug(uri);

        List<String> labels = new ArrayList<>();
        labels.add(Props.CURATED);
        if (label != null) {
            String filterLabel = label;
            try {
                filterLabel = URLDecoder.decode(label, "UTF-8");
            } catch (UnsupportedEncodingException e) {
            }
            if (filterLabel.charAt(0) == '"' && filterLabel.charAt(filterLabel.length()-1) == '"')
                filterLabel = filterLabel.substring(1, filterLabel.length()-1);
            labels.add(filterLabel);
        }

        Entity[] entities;
        int s = skip != null ? skip : 0;
        int t = top != null ? Math.min(top,1000) : 10;

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

        ArrayNode entries = mapper.createArrayNode();
        for (Entity e : entities) {
            ObjectNode entry = mapper.createObjectNode();
            entry.put("id", e.getId());
            entry.put("source", e.datasource().getKey());
            entry.put("datasource", e.datasource().getName());
            Map<String, Object> payload = e.payload();
            if (e.payload().containsKey("_CURATION")) {
                Object curation = e.payload().get("_CURATION");
                ArrayNode an = mapper.createArrayNode();
                if (curation.getClass().isArray())
                    for (int i=0; i<Array.getLength(curation); i++)
                        an.add(Array.get(curation, i).toString());
                entry.put("_CURATION", an);
            }
            entries.add(entry);
        }

        ObjectNode result = mapper.createObjectNode();
        result.put("skip", skip);
        result.put("top", top);
        result.put("count", entities.length);
        result.put("uri", request().uri());
        result.put("contents", entries);

        return ok(result);
    }

    public Result updateLatestStitch (String id) {
        return updateLatestStitch (id, false);
    }

    public Result testUpdateLatestStitch (String id) {
        return updateLatestStitch (id, true);
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
