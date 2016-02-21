package services.jobs;

import javax.inject.Inject;
import play.Logger;

import org.quartz.Job;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

import ix.curation.GraphDb;
import services.GraphDbService;

public abstract class RegistrationJob implements Job, JobParams {
    @Inject protected GraphDbService service;
    
    public RegistrationJob () {
    }

    public GraphDb getGraphDb () { return service.getGraphDb(); }
}
