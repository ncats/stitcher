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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import services.GraphDbService;
import services.SchedulerService;
import services.CacheService;
import services.jobs.*;
import utils.JsonUtil;

import ix.curation.*;

public class Api extends Controller {
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
            if (null != ds.toURI()) {
                node.put("uri", ds.toURI().toString());
            }
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
            sources.add(node);
        }
        return ok (sources);
    }

    public Result getMetrics (final String label) {
        final String key = routes.Api.getMetrics(label).toString();
        try {
            Object value = cache.getOrElse
                (graphDb.getEntityFactory().getLastUpdated(), key,
                 new Callable () {
                     public Object call () throws Exception {
                         Map<String, Object> params =
                             new HashMap<String, Object>();
                         if (label != null)
                             params.put(JobParams.LABEL, label);
                         params.put(JobParams.KEY, key);
                         return scheduler.submit(CalcMetricsJob.class, params);
                     }
                 });
            if (value instanceof CurationMetrics) {
                return ok ((JsonNode)mapper.valueToTree(value));
            }
            ObjectNode node = mapper.createObjectNode();
            node.put("id", mapper.valueToTree(value));
            node.put("uri", request().uri());
            node.put("status", "Calculating metrics...");
            return ok (node);
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result getResult (String id) {
        Object result = cache.get(id);
        if (result != null) {
            return ok ((JsonNode)mapper.valueToTree(result));
        }
        return notFound ("Unknown result key "+id);
    }
}
