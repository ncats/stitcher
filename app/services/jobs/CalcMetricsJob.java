package services.jobs;

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
public class CalcMetricsJob implements Job, JobParams {
    @Inject protected GraphDbService service;
    @Inject protected CacheService cache;

    public CalcMetricsJob () {
    }

    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        try {
            JobDataMap params = ctx.getMergedJobDataMap();
            String key = (String)params.get(KEY);
            String label = (String)params.get(LABEL);
            EntityFactory efac = service.getEntityFactory();
            long start = System.currentTimeMillis();
            CurationMetrics metrics =
                label != null ? efac.calcCurationMetrics(label)
                : efac.calcCurationMetrics();
            long elapsed = System.currentTimeMillis() - start;
            ctx.setResult(metrics);
            cache.set(key, metrics);
            Logger.debug(Thread.currentThread().getName()+": key="+key
                         +" job "+getClass().getName()+" executed in "
                         +elapsed+"ms!");
        }
        catch (Exception ex) {
            throw new JobExecutionException (ex);
        }
    }
}
