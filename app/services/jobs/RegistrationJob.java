package services.jobs;

import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeEvent;

import javax.inject.Inject;
import play.Logger;

import org.quartz.Job;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

import ix.curation.GraphDb;
import services.GraphDbService;

public abstract class RegistrationJob
    implements Job, JobParams, PropertyChangeListener {
    
    @Inject protected GraphDbService service;
    
    protected int count;
    protected int error;
    
    public RegistrationJob () {
    }

    public GraphDb getGraphDb () { return service.getGraphDb(); }
    public int getCount () { return count; }
    public int getError () { return error; }

    public void propertyChange (PropertyChangeEvent ev) {
        String prop = ev.getPropertyName();
        if ("entity".equals(prop)) {
            ++count;
        }
        else if ("error".equals(prop)) {
            ++error;
        }
        /*
        Logger.debug(Thread.currentThread().getName()
                     +": "+String.format("%1$ 5d", count)+" "
                     +ev.getPropertyName()+" old="+ev.getOldValue()
                     +" new="+ev.getNewValue());
        */
    }
}
