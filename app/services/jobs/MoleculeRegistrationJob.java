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
import org.quartz.PersistJobDataAfterExecution;

import ix.curation.impl.MoleculeEntityFactory;
import ix.curation.GraphDb;
import ix.curation.StitchKey;
import ix.curation.DataSource;

import services.GraphDbService;

public class MoleculeRegistrationJob extends RegistrationJob {
    
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
            long start = System.currentTimeMillis();
            DataSource ds = mef.register(file);
            String elapsed = String.format
                ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start));
            
            Logger.debug(Thread.currentThread().getName()
                         +": job "+jobid+" registered "+ds+" in "+elapsed);
            ctx.setResult(ds);
        }
        else {
            // now try URL
            URL url = (URL)map.get(URL);
            if (url != null) {
                long start = System.currentTimeMillis();
                DataSource ds = mef.register(url);
                String elapsed = String.format
                    ("%1$.3fs", 1e-3*(System.currentTimeMillis()-start));
                Logger.debug(Thread.currentThread().getName()+": job "
                             +jobid +" registered "+ds+" in "+elapsed);
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
    
    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        try {
            MoleculeEntityFactory mef = getEntityFactory ();
            mef.addPropertyChangeListener(this);
            execute (mef, ctx);
        }
        catch (Exception ex) {
            throw new JobExecutionException (ex);
        }
    }
}
