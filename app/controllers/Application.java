package controllers;

import java.util.*;
import java.io.*;
import javax.inject.*;
import java.net.URI;

import play.*;
import play.mvc.*;
import play.libs.ws.*;

import views.html.*;

import services.GraphDbService;
import services.SchedulerService;
import services.jobs.*;

import ix.curation.*;

public class Application extends Controller {
    //@Inject WSClient ws;
    @Inject SchedulerService scheduler;
    @Inject play.Application app;
    @Inject GraphDbService graphDb;

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
            return ok (key);
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
            return ok (key);
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
            return ok (key);
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
            return ok (key);
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }
}
