package services;

import java.io.*;
import java.sql.*;
import java.util.*;

import javax.inject.Inject;
import javax.inject.Singleton;

import org.h2.tools.RunScript;

import play.Application;
import play.Logger;
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

@Singleton
public class SchedulerService {
    static final String QUARTZ = "quartz";
    static final String QUARTZ_INIT = "ix.quartz.init";
    static final String QUARTZ_THREADS = "ix.quartz.threads";
    static final String JOB_GROUP = SchedulerService.class.getName();

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
            Logger.debug(getJobCount()+" job(s) waiting in queue!");
            
            lifecycle.addStopHook(() -> {
                    shutdown ();
                    return F.Promise.pure(null);
                });
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public Set<String> getJobs () {
        Set<String> jobs = new TreeSet<String>();
        try {
            for (JobKey jk : scheduler.getJobKeys
                     (GroupMatcher.groupEquals(JOB_GROUP))) {
                jobs.add(jk.getName());
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
                  "services.SchedulerService$QuartzConnectionProvider");
        props.put("org.quartz.jobStore.dataSource", "quartzDS");
        return props;
    }

    static Scheduler createScheduler (Application app) throws Exception {
        SchedulerFactory schedulerFactory =
            new StdSchedulerFactory (createSchedulerConfig (app));
        Scheduler scheduler = schedulerFactory.getScheduler();
        Logger.debug("Scheduler initialized..."+scheduler);
        return scheduler;
    }
}
