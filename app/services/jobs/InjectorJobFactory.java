package services.jobs;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import javax.inject.Inject;
import play.Logger;
import play.inject.Injector;

public class InjectorJobFactory implements JobFactory {
    private final Injector injector;

    public InjectorJobFactory (Injector injector) {
        this.injector = injector;
    }

    @Override
    public Job newJob (TriggerFiredBundle triggerFiredBundle,
                       Scheduler scheduler) throws SchedulerException {
        // Get the job detail so we can get the job class
        JobDetail jobDetail = triggerFiredBundle.getJobDetail();
        Class jobClass = jobDetail.getJobClass();

        return (Job) injector.instanceOf(jobClass);
    }
}
