package controllers.test;

import java.util.*;
import java.io.*;
import javax.inject.*;

import play.*;
import play.mvc.*;
import play.libs.ws.*;

import views.html.*;

import services.SchedulerService;
import services.jobs.*;


public class Test extends Controller {
    @Inject public SchedulerService scheduler;
    @Inject play.Application app;

    public Test () {
    }
    
    public Result slidereveal () {
        return ok (slidereveal.render("IxCurator"));
    }

    public Result drugbank () {
        try {
            String key = scheduler.submit
                (DrugBankMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/drugbank-full-annotated.sdf"));
            return redirect (controllers.app.routes.App.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result npc () {
        try {
            String key = scheduler.submit
                (NPCMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"));
            return redirect (controllers.app.routes.App.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result srs () {
        try {
            String key = scheduler.submit
                (SRSJsonRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/public2015-11-30.gsrs"));
            return redirect (controllers.app.routes.App.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }

    public Result integrity () {
        try {
            String key = scheduler.submit
                (IntegrityMoleculeRegistrationJob.class, null,
                 app.getFile("../inxight-planning/files/integr.sdf.gz"));
            return redirect (controllers.app.routes.App.console(key));
        }
        catch (Exception ex) {
            return internalServerError (ex.getMessage());
        }
    }
}
