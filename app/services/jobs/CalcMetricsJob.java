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

import ncats.stitcher.EntityFactory;
import ncats.stitcher.GraphDb;
import ncats.stitcher.GraphMetrics;

import services.EntityService;
import services.CacheService;

@DisallowConcurrentExecution
@StartUpJob
public class CalcMetricsJob implements Job, JobParams {
    static final AtomicLong lastRun = new AtomicLong ();
    
    @Inject protected EntityService service;
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
                Object metrics = service.calcMetrics();
                ctx.setResult(metrics);
                cache.set(METRICS, metrics, 0);
                lastRun.set(System.currentTimeMillis());
            }
            else {
                Object metrics = cache.get(METRICS);
                if (metrics == null) {
                    metrics = service.calcMetrics();
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
