package services.jobs;

import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;
import play.Logger;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;
import org.quartz.DisallowConcurrentExecution;

import ix.curation.EntityFactory;
import ix.curation.GraphDb;
import ix.curation.CurationMetrics;

import services.GraphDbService;
import services.CacheService;

@DisallowConcurrentExecution
@StartUpJob
public class CalcMetricsJob implements Job, JobParams {
    static final AtomicLong lastRun = new AtomicLong ();
    
    @Inject protected GraphDbService service;
    @Inject protected CacheService cache;

    public CalcMetricsJob () {
    }

    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        try {
            long start = System.currentTimeMillis();
            JobDataMap params = ctx.getMergedJobDataMap();          
            Logger.debug(Thread.currentThread().getName()
                         +": job "+getClass().getName()
                         +" triggered...");

            if (lastRun.get() < service.getLastUpdated()) {
                Object metrics = service.getEntityFactory()
                    .calcCurationMetrics();
                ctx.setResult(metrics);
                cache.set(METRICS, metrics, 0);
                lastRun.set(System.currentTimeMillis());
            }
            else {
                Object metrics = cache.get(METRICS);
                if (metrics == null) {
                    metrics = service.getEntityFactory()
                        .calcCurationMetrics();
                    cache.set(METRICS, metrics, 0);
                    lastRun.set(System.currentTimeMillis());
                }
                ctx.setResult(metrics);
            }
            long elapsed = System.currentTimeMillis() - start;
            Logger.debug(Thread.currentThread().getName()
                         +": job "+getClass().getName()
                         +" finished in "+String.format("%1$dms", elapsed));
        }
        catch (Exception ex) {
            throw new JobExecutionException (ex);
        }
    }
}
