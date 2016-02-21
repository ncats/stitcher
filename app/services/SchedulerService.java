package services;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.net.URL;

import javax.inject.Singleton;
import javax.inject.Inject;

import org.h2.tools.RunScript;
import play.Application;
import play.Logger;
import play.inject.Injector;
import play.db.*;
import play.libs.F;
import play.inject.ApplicationLifecycle;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.utils.ConnectionProvider;

import static org.quartz.JobBuilder.*; 
import static org.quartz.SimpleScheduleBuilder.*; 
import static org.quartz.CronScheduleBuilder.*; 
import static org.quartz.CalendarIntervalScheduleBuilder.*; 
import static org.quartz.TriggerBuilder.*; 
import static org.quartz.DateBuilder.*;

import services.jobs.*;

@Singleton
public class SchedulerService {
    static final String QUARTZ = "quartz";
    static final String QUARTZ_INIT = "ix.quartz.init";
    static final String QUARTZ_THREADS = "ix.quartz.threads";

    static public class QuartzConnectionProvider
        implements ConnectionProvider {
        public QuartzConnectionProvider () {}

        public Connection getConnection () throws SQLException {
            return DB.getConnection(QUARTZ);
        }
        public void initialize () throws SQLException {}
        public void shutdown () throws SQLException {}
    }

    Scheduler scheduler;

    @Inject
    public SchedulerService (Application app,
                             ApplicationLifecycle lifecycle) {
        try {
            initDb (app);
            scheduler = createScheduler (app);
            scheduler.start(); // now start the scheduler
            Logger.debug(getJobCount()+" job(s) in queue!");

            submit (SRSJsonRegistrationJob.class, null,
                    app.getFile("../inxight-planning/files/public2015-11-30.gsrs"));
            submit (IntegrityMoleculeRegistrationJob.class, null,
                    app.getFile("../inxight-planning/files/integr.sdf.gz"));
            /*
            registerJob (app.getFile("../inxight-planning/files/drugbank-full-annotated.sdf"));

            registerJob (app.getFile("../inxight-planning/files/npc-dump-1.2-04-25-2012_annot.sdf.gz"));
            */
            
            lifecycle.addStopHook(() -> {
                    shutdown ();
                    return F.Promise.pure(null);
                });
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }
    
    void deleteJobs () throws Exception {
        for (JobKey job : getJobs ()) {
            scheduler.deleteJob(job);
        }
    }

    void addJob () throws Exception {
        JobDetail job = JobBuilder
            .newJob(MoleculeRegistrationJob.class)
            //.withIdentity(MoleculeRegistrationJob.class.getName())
            //.storeDurably(true)
            .requestRecovery(true)
            .build();
        Trigger trigger = TriggerBuilder.
            newTrigger()
            .startNow()
            //.startAt(new java.util.Date (System.currentTimeMillis()+5*60*60))
            .build();
        scheduler.scheduleJob(job, trigger);
    }

    void triggerJobs () throws Exception {
        for (JobKey jk : getJobs ()) {
            Trigger trigger = TriggerBuilder
                .newTrigger()
                .forJob(jk)
                .startNow()
                .build();
            scheduler.scheduleJob(trigger);
        }
    }

    public String submit (Class<? extends RegistrationJob> jobClass,
                          Map<String, Object> params,
                          File file) throws SchedulerException {
        JobDetail job = JobBuilder
            .newJob(jobClass)
            .requestRecovery(true)
            .build();
        if (params != null) {
            for (Map.Entry<String, Object> me : params.entrySet()) {
                Object value = me.getValue();
                if (value instanceof Serializable) {
                    job.getJobDataMap().put(me.getKey(), value);
                }
                else {
                    Logger.warn("Job "+jobClass+"; ignoring parameter \""
                                +me.getKey()
                                +"\" because value is not serializable!");
                }
            }
        }
        job.getJobDataMap().put(RegistrationJob.FILE, file);
        
        Trigger trigger = TriggerBuilder
            .newTrigger()
            .startNow()
            .build();
        
        scheduler.scheduleJob(job, trigger);
        return job.getKey().getName();
    }

    public String submit (Class<? extends RegistrationJob> jobClass,
                          Map<String, Object> params,
                          URL url) throws SchedulerException {
        JobDetail job = JobBuilder
            .newJob(jobClass)
            .requestRecovery(true)
            .build();
        if (params != null) {
            for (Map.Entry<String, Object> me : params.entrySet()) {
                Object value = me.getValue();
                if (value instanceof Serializable) {
                    job.getJobDataMap().put(me.getKey(), value);
                }
                else {
                    Logger.warn("Job "+jobClass+"; ignoring parameter \""
                                +me.getKey()
                                +"\" because value is not serializable!");
                }
            }
        }
        job.getJobDataMap().put(RegistrationJob.URL, url);
        
        Trigger trigger = TriggerBuilder
            .newTrigger()
            .startNow()
            .build();
        
        scheduler.scheduleJob(job, trigger);
        return job.getKey().getName();
    }

    public Set<JobKey> getJobs () {
        Set<JobKey> jobs = new TreeSet<JobKey>();
        try {
            for (JobKey jk :
                     scheduler.getJobKeys(GroupMatcher.anyJobGroup())) {
                jobs.add(jk);
            }
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
        
        return jobs;
    }

    public int getJobCount () { return getJobs().size(); }

    public void shutdown () {
        if (scheduler != null) {
            Logger.debug("Shutting down "+getClass()+"..."+scheduler);
            try {
                scheduler.shutdown(true);
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    static void initDb (Application app) throws Exception {
        try (Connection con = DB.getConnection(QUARTZ)) {
            Statement stmt = con.createStatement();
            ResultSet rset = stmt.executeQuery
                ("select count(*) from information_schema.tables "
                 +"where table_name like 'QRTZ_%'");
            
            int tables = 0;
            if (rset.next()) {
                tables = rset.getInt(1);
            }
            rset.close();
            stmt.close();

            if (tables == 0) {
                String quartz = app.configuration().getString(QUARTZ_INIT);
                if (app.isProd()) {
                    RunScript.execute(con, new InputStreamReader
                                      (app.resourceAsStream(quartz)));
                }
                else {
                    File file = app.getFile("conf/"+quartz);
                    Logger.debug("Running script "+file+"...");
                    RunScript.execute(con, new FileReader (file));
                }
            }
            Logger.debug(SchedulerService.class+ "... initialized!");
        }
    }

    static Properties createSchedulerConfig (Application app) {
        Properties props = new Properties ();
        props.put("org.quartz.threadPool.threadCount",
                  app.configuration().getString(QUARTZ_THREADS, "4"));
        props.put("org.quartz.jobStore.class",
                  "org.quartz.impl.jdbcjobstore.JobStoreTX");
        props.put("org.quartz.jobStore.driverDelegateClass",
                  "org.quartz.impl.jdbcjobstore.StdJDBCDelegate");
        props.put("org.quartz.dataSource.quartzDS.connectionProvider.class",
                  QuartzConnectionProvider.class.getName());
        props.put("org.quartz.jobStore.dataSource", "quartzDS");
        return props;
    }

    static Scheduler createScheduler (Application app)
        throws Exception {
        SchedulerFactory schedulerFactory =
            new StdSchedulerFactory (createSchedulerConfig (app));
        Scheduler scheduler = schedulerFactory.getScheduler();
        scheduler.setJobFactory(new InjectorJobFactory (app.injector()));
        
        Logger.debug("Scheduler initialized..."+scheduler);
        return scheduler;
    }
}
