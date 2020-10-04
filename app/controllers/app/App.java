package controllers.app;

import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;

import play.*;
import play.mvc.*;
import play.data.*;
import play.libs.ws.*;
import static play.mvc.Http.MultipartFormData.*;
import play.db.ebean.Transactional;
import org.webjars.play.WebJarsUtil;

import views.html.*;

import services.EntityService;
import services.SchedulerService;
import services.CoreService;
import services.jobs.*;

import controllers.Util;
import org.neo4j.graphdb.Label;
import chemaxon.struc.Molecule;

import ncats.stitcher.*;
import static ncats.stitcher.BuildInfo.*;
import models.*;

public class App extends Controller {
    @Inject public SchedulerService scheduler;
    @Inject public EntityService es;
    @Inject public CoreService service;
    @Inject public WebJarsUtil webjars;
    
    public App () {
    }

    //public controllers.WebJarAssets webjars () { return webJarAssets; }    

    /*
    public Result build () {
        return ok(welcome.render("Build: "+TIME+" ("+BRANCH+"-"+COMMIT+")"));
    }
    */

    public Result console (String key) {
        return ok (console.render(this, key));
    }

    public Result uploadForm () {
        return ok (upload.render(this));
    }

    @Transactional
    @BodyParser.Of(value = BodyParser.MultipartFormData.class)
    public Result upload () {
        Http.MultipartFormData body = request().body().asMultipartFormData();
        Map<String, String[]> params = body.asFormUrlEncoded();
        FilePart part = body.getFile("file");
        //Logger.debug("part="+part+" uri="+params.get("uri"));
        if (part != null) {
            try {
                models.Payload payload = service.upload(part, params);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                flash ("error", ex.getMessage());
                return redirect (routes.App.uploadForm());
            }
        }
        else {
            String[] uri = params.get("uri");
            if (uri == null || uri.length == 0 || uri[0].equals("")) {
                flash ("error", "Either File and/or URI must be specified!");
                return redirect (routes.App.uploadForm());
            }
            try {
                models.Payload payload = service.upload
                    (new URI (uri[0]), params);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                
                flash ("error", ex.getMessage());
                return redirect (routes.App.uploadForm());              
            }
        }
        
        return redirect (routes.App.payload());
    }

    public Result payload () {
        List<models.Payload> payloads = service.getPayloads();
        return ok (payload.render(this, payloads));
    }

    public Result getPayload (String key) {
        models.Payload payload = service.getPayload(key);
        return ok (payload != null ? payloaddetails.render(this, payload)
                   : error.render(this, "Invalid payload: "+key));
    }

    public Result setup () {
        return ok (payloadsetup.render(this));
    }

    public Result index () {
        return redirect (routes.App.stitches(1, null, 5, 0));
    }
    
    public Result dashboard () {
        return ok (dashboard.render(this));
    }

    public Result delete (String key) {
        models.Payload payload = service.deletePayload(key);
        if (payload != null) {
            flash ("message", "Successfully deleted payload "+payload.sha1());
        }
        return redirect (routes.App.payload());
    }

    public Result latestStitches (String q, Integer rows, Integer page) {
        String uri = routes.App.latestStitches(q, rows, page).url();
        Integer version = service.getLatestVersion();
        if (null == version)
            return ok (error.render(this, uri+": No latest version defined!"));

        return stitches (version, q, rows, page);
    }
    
    public Result stitches (Integer version, String q,
                            Integer rows, Integer page) {
        Label[] labels = {
            AuxNodeType.SGROUP,
            Label.label("S_STITCH_V"+version)
        };
        String uri = routes.App.stitches(version, q, rows, page).url();
        Logger.debug(uri);
        
        Long total = es.getEntityFactory().count(labels);
        if (total == null) {
            return ok (error.render(this, "Internal server error: "+uri));
        }
        else if (total == 0) {
            return ok (notfound.render(uri));
        }
        else {
            int[] pages = Util.paging(rows, page, total.intValue());
            Entity[] entities = es.getEntityFactory()
                .entities((page-1)*rows, rows, labels);
            
            return ok (stitches.render
                       (version, q, pages, page, rows, total.intValue(),
                        Arrays.stream(entities).map(e -> Stitch.getStitch(e))
                        .toArray(Stitch[]::new)));
        }
    }

    public Result stitch (Integer version, String name) {
        String uri = routes.App.stitch(version, name).url();
        Logger.debug(uri);
        
        Stitch stitch = null;
        try {
            long pid = Long.parseLong(name);
            Entity e = es.getEntityFactory().entity(pid);
            if (!e.is(AuxNodeType.SGROUP))
                return ok (notfound.render("Entity "+name
                                           +" is not of type stitch!"));

            return ok (stitchdetails.render
                       (this, version, Stitch.getStitch(e)));
        }
        catch (Exception ex) {
            // now try looking by name or id
            Entity[] entities = es.getEntityFactory()
                .filter("id", "'"+name+"'", "S_STITCH_V"+version);
            
            return ok (stitches.render
                       (version, name, new int[]{entities.length},
                        1, entities.length, entities.length,
                        Arrays.stream(entities).map(e -> Stitch.getStitch(e))
                        .toArray(Stitch[]::new)));
        }
        //return ok (error.render(this, uri+": can find entity "+name));
    }

    public Result latestStitch (String name) {
        Integer version = service.getLatestVersion();
        if (null == version)
            return badRequest ("No latest version defined!");

        return stitch (version, name);
    }

    public Result structure (Long id, Integer size ) {
	Entity e = es.getEntityFactory().getEntity(id);
	if (e != null) {
	    return ok (structure.render(e, size));
	}
	return badRequest ("Unknown entity: "+id);
    }
}
