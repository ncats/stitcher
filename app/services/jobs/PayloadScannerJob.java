package services.jobs;

import java.io.*;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;
import play.Logger;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

import models.Payload;
import services.CoreService;
import services.RecordReader;

public class PayloadScannerJob implements Job, JobParams {
    @Inject protected CoreService service;
    
    public PayloadScannerJob () {}

    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        String key = ctx.getTrigger().getKey().getName();
        
        JobDataMap params = ctx.getMergedJobDataMap();
        long id = params.getLongValue("payload");
        try {
            long start = System.currentTimeMillis();
            Payload payload = Payload.find.byId(id);
            File file = service.getFile(payload);
            if (file == null) {
                throw new RuntimeException
                    ("Payload "+payload.id+" has no associated file!");
            }

            InputStream is;
            if (payload.magic != null) {
                if (payload.magic == GZIPInputStream.GZIP_MAGIC) {
                    is = new GZIPInputStream (new FileInputStream (file));
                }
                else
                    throw new RuntimeException
                        ("Unknown magic "+payload.magic
                         +" found in payload "+payload.id);
            }
            else
                is = new FileInputStream (file);

            is.close();
            Logger.debug("Job "+key+" finished in "
                         +(System.currentTimeMillis()-start)+"ms!");
        }
        catch (Exception ex) {
            Logger.trace("Can't execute job "+key, ex);
            throw new JobExecutionException (ex);
        }
    }
}
