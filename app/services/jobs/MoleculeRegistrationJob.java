package services.jobs;

import javax.inject.Inject;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeEvent;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import akka.actor.ActorRef;
import akka.actor.PoisonPill;

import play.Logger;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.JobBuilder;
import org.quartz.TriggerBuilder;
import org.quartz.Trigger;
import org.quartz.DateBuilder;

import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.DataSource;
import ix.curation.Entity;

import services.GraphDbService;
import services.CacheService;
import services.WebSocketConsoleActor;

public class MoleculeRegistrationJob extends RegistrationJob {

    @Inject CacheService cache;
    String key; // current executing context key
    
    public MoleculeRegistrationJob () {
    }

    protected void execute (MoleculeEntityFactory mef, JobExecutionContext ctx)
        throws Exception {
        JobDetail job = ctx.getJobDetail();
        String jobid = job.getKey().getName();
            
        Logger.debug(Thread.currentThread().getName()
                     +": ctx="+ctx+" job "+jobid+" triggered...");

        JobDataMap map = ctx.getMergedJobDataMap();        
        for (Map.Entry<String, Object> me : map.entrySet()) {
            Object value = me.getValue();
            if (value == null) {
                // ignore
            }
            else if (ID.equals(me.getKey())) {
                mef.setId(value.toString());
            }
            else {
                try {
                    StitchKey key = StitchKey.valueOf(me.getKey());
                    mef.add(key, value.toString());
                }
                catch (Exception ex) {
                    // not a stitchkey, so ignore
                }
            }
        }
        
        File file = (File)map.get(FILE);
        if (file != null) {
            DataSource ds = mef.register(file);
            ctx.setResult(ds);
        }
        else {
            // now try URL
            URL url = (URL)map.get(URL);
            if (url != null) {
                DataSource ds = mef.register(url);
                ctx.setResult(ds);
            }
            else {
                // what should we do now?
                throw new JobExecutionException
                    ("Neither \""+FILE+"\" nor \""+URL+"\" parameter set!");
            }
        }
    }

    protected MoleculeEntityFactory getEntityFactory () throws IOException {
        return new MoleculeEntityFactory (getGraphDb ());
    }

    void submitMetricsJob (Scheduler scheduler) throws SchedulerException {
        JobDetail job = JobBuilder
            .newJob(CalcMetricsJob.class)
            .build();

        Trigger trigger = TriggerBuilder
            .newTrigger()
            .usingJobData(KEY, METRICS)
            .startAt(DateBuilder.futureDate
                     (10, DateBuilder.IntervalUnit.SECOND))
            .build();
        
        scheduler.scheduleJob(job, trigger);
    }
    
    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        this.key = ctx.getTrigger().getKey().getName();
        try {
            MoleculeEntityFactory mef = getEntityFactory ();
            mef.addPropertyChangeListener(this);
            long start = System.currentTimeMillis();        
            execute (mef, ctx);
            String elapsed = String.format
                ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start));
            Logger.debug(Thread.currentThread().getName()+": job "
                         +key +" registered "+ctx.getResult()+" in "+elapsed);
            mef.removePropertyChangeListener(this);
            ActorRef actor = (ActorRef)cache.get(key);
            if (actor != null) {
                actor.tell(WebSocketConsoleActor.message
                           ("finished processing \""
                            +ctx.getResult()+"\" in "+elapsed),
                           ActorRef.noSender());
                actor.tell(PoisonPill.getInstance(), ActorRef.noSender());
            }

            // now schedule the CaclMetricsJob
            submitMetricsJob (ctx.getScheduler());
        }
        catch (Exception ex) {
            throw new JobExecutionException (ex);
        }
    }

    @Override
    public void propertyChange (PropertyChangeEvent ev) {
        super.propertyChange(ev);
        ActorRef actor = (ActorRef)cache.get(key);

        if (actor != null) {
            String prop = ev.getPropertyName();
            if (prop.equalsIgnoreCase("entity")) {
                Entity e = (Entity)ev.getNewValue();
                actor.tell(WebSocketConsoleActor.message
                           (String.format("%1$ 5d: entity %2$d added...",
                                          getCount(), e.getId())),
                           ActorRef.noSender());
            }
            else if (prop.equalsIgnoreCase("error")) {
                Entity e = (Entity)ev.getOldValue();
                actor.tell(WebSocketConsoleActor.message
                           ("ERROR: processing entity "
                            +e.getId()+": "+ev.getNewValue()),
                           ActorRef.noSender());
            }
        }
    }
}
