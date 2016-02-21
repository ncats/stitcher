package services.jobs;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;

import play.Logger;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.DataSource;

import services.GraphDbService;

public class MoleculeRegistrationJob extends RegistrationJob {
    public MoleculeRegistrationJob () {
    }

    public GraphDb getGraphDb () { return service.getGraphDb(); }

    protected void execute (MoleculeEntityFactory mef, JobExecutionContext ctx)
        throws Exception {
        JobDetail job = ctx.getJobDetail();
        String jobid = getClass().getName()+":"+job.getKey().getName();
            
        Logger.debug(Thread.currentThread().getName()
                     +": job "+jobid+" triggered..."+getGraphDb ());

        JobDataMap map = job.getJobDataMap();
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
        
        File file = (File)job.getJobDataMap().get(FILE);
        if (file != null) {
            DataSource ds = mef.register(file);
            Logger.debug("Job "+job.getKey().getName()+": registered "+ds);
        }
        else {
            // now try URL
            URL url = (URL)job.getJobDataMap().get(URL);
            if (url != null) {
                DataSource ds = mef.register(url);
                Logger.debug("Job "+job.getKey().getName()
                             +": registered "+ds);
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
    
    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        try {
            execute (getEntityFactory (), ctx);
        }
        catch (Exception ex) {
            throw new JobExecutionException (ex);
        }
    }
}
