package controllers;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;

import views.html.*;
import services.GraphDbService;
import services.SchedulerService;

import ix.curation.GraphDb;

public class Application extends Controller {
    final GraphDb graphDb;
    @Inject WSClient ws;
    @Inject SchedulerService scheduler;

    @Inject
    public Application (GraphDbService service) {
        this.graphDb = service.getGraphDb();
    }
    
    public Result build () {
        return ok(welcome.render("Build: "+ix.BuildInfo.TIME+" ("
                                 +ix.BuildInfo.BRANCH+"-"
                                 +ix.BuildInfo.COMMIT+")"));
    }

    public Result slidereveal () {
        return ok (slidereveal.render("IxCurator"));
    }
}
