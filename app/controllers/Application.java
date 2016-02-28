package controllers;

import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;

import play.*;
import play.mvc.*;
import play.libs.ws.*;
import static play.mvc.Http.MultipartFormData.*;
import play.db.ebean.Transactional;

import views.html.*;

import services.GraphDbService;
import services.SchedulerService;
import services.CoreService;
import services.jobs.*;

import ix.curation.*;
import models.*;

public class Application extends Controller {
    //@Inject WSClient ws;
    @Inject SchedulerService scheduler;
    @Inject play.Application app;
    @Inject GraphDbService graphDb;
    @Inject CoreService service;

    public Application () {
    }
    
    public Result build () {
        return ok(welcome.render("Build: "+ix.BuildInfo.TIME+" ("
                                 +ix.BuildInfo.BRANCH+"-"
                                 +ix.BuildInfo.COMMIT+")"));
    }

    public Result slidereveal () {
        return ok (slidereveal.render("IxCurator"));
    }

    public Result testDrugBank () {
        try {
            String key = scheduler.submit
                (DrugBankMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/drugbank-full-annotated.sdf"));
            return redirect (routes.Application.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result testNPC () {
        try {
            String key = scheduler.submit
                (NPCMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"));
            return redirect (routes.Application.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result testSRS () {
        try {
            String key = scheduler.submit
                (SRSJsonRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/public2015-11-30.gsrs"));
            return redirect (routes.Application.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result testIntegrity () {
        try {
            String key = scheduler.submit
                (IntegrityMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/integr.sdf.gz"));
            return redirect (routes.Application.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result console (String key) {
        return ok (console.render(key));
    }

    public Result uploadForm () {
        return ok (upload.render());
    }

    @Transactional
    @BodyParser.Of(value = BodyParser.MultipartFormData.class, 
                   maxLength = 1024*1024*1000000)
    public Result upload () {
        if (request().body().isMaxSizeExceeded()) {
            return badRequest (error.render("File too large!"));
        }

        Http.MultipartFormData body = request().body().asMultipartFormData();
        FilePart part = body.getFile("file");
        if (part != null) {
            try {
                models.Payload payload = service.upload
                    (part, body.asFormUrlEncoded());
            }
            catch (Exception ex) {
                flash ("error", ex.getMessage());
                return redirect (routes.Application.uploadForm());
            }
        }
        
        return redirect (routes.Application.payload());
    }

    public Result payload () {
        List<models.Payload> payloads = models.Payload
            .find.order().desc("id").findList();
        
        return ok (payload.render(payloads));
    }
}
