package services.jobs;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import javax.inject.Inject;
import play.Logger;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionException;
import org.quartz.JobExecutionContext;

import models.Payload;
import models.Property;
import services.CoreService;
import services.RecordScanner;
import services.DelimiterRecordScanner;
import services.MolRecordScanner;

import com.avaje.ebean.Ebean;
import com.avaje.ebean.Transaction;

public class PayloadScannerJob implements Job, JobParams {
    @Inject protected CoreService service;
    
    public PayloadScannerJob () {}

    public void execute (JobExecutionContext ctx)
        throws JobExecutionException {
        String key = ctx.getTrigger().getKey().getName();
        JobDataMap params = ctx.getMergedJobDataMap();

        if (!params.containsKey(PAYLOAD))
            throw new JobExecutionException
                ("Job "+getClass().getName()+" requires parameter "
                 +PAYLOAD+"!");
        
        long id = params.getLong(PAYLOAD);

        long start = System.currentTimeMillis();        
        Transaction tx = Ebean.beginTransaction();
        try {
            Payload payload = Payload.find.byId(id);
            File file = service.getFile(payload);
            if (file == null) {
                throw new RuntimeException
                    ("Payload "+payload.id+" has no associated file!");
            }

            InputStream is;
            try {
                is = new GZIPInputStream (new FileInputStream (file));
            }
            catch (Exception ex) {
                // perhaps not gzip, 
                is = new FileInputStream (file);
            }

            RecordScanner scanner = null;
            switch (payload.format) {
            case "CSV": case "csv":
                scanner = new DelimiterRecordScanner.CSV();
                break;
                
            case "TXT": case "txt":
                if (params.containsKey(DELIMITER)) {
                    scanner = new DelimiterRecordScanner
                        (params.getString(DELIMITER));
                    break;
                }
                // fall through assuming tab delimited
                
            case "TSV": case "tsv":
                scanner = new DelimiterRecordScanner.TSV();
                break;

            case "MOL": case "SDF": case "SMI": case "SMILES":
            case "mol": case "sdf": case "smi": case "smiles":
                scanner = new MolRecordScanner ();
                break;

            default:
                Logger.error("Unknown payload format: "+payload.format);
            }

            if (scanner != null) {
                Logger.debug("Running payload scanner "+scanner+"...");
                scanner.setInputStream(is);
                scanner.setMaxScanned(-1); // scan everything
                scanner.scan();

                String[] props = scanner.getProperties();
                Logger.debug("properties..."+props.length);

                for (String prop : props) {
                    if (prop != null) {
                        Property p = new Property (prop);
                        p.setCount(scanner.getCount(prop));
                        Class cls = scanner.getType(p.name);
                        if (cls != null)
                            p.setType(cls.getSimpleName());
                        Logger.debug("... "+p.name+" "+p.type+" "+p.count);
                        payload.properties.add(p);
                    }
                }
                payload.setCount(scanner.getCount());
                payload.save();
                tx.commit();
                
                ctx.setResult(payload);
            }

            is.close();
            Logger.debug("Job "+key+" finished in "
                         +(System.currentTimeMillis()-start)+"ms!");
        }
        catch (Exception ex) {
            Logger.trace("Can't execute job "+key, ex);
            throw new JobExecutionException (ex);
        }
        finally {
            tx.end();
        }
    }
}
