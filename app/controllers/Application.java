package controllers;

import javax.inject.*;
import play.*;
import play.mvc.*;
import play.libs.ws.*;

import views.html.*;
import services.GraphDbService;

import ix.curation.GraphDb;

public class Application extends Controller {
    final GraphDb graphDb;
    @Inject WSClient ws;

    @Inject
    public Application (GraphDbService service) {
        this.graphDb = service.getGraphDb();
    }
    
    public Result welcome () {
        return ok(welcome.render("Your new application is ready..."
                                 +graphDb+" ws="+ws));
    }

    public Result slidereveal () {
        return ok (slidereveal.render("IxCurator"));
    }
}
