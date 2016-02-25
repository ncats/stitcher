package services.jobs;

import java.lang.annotation.*;

@Documented
@Retention(value=RetentionPolicy.RUNTIME)
@Target(value={ElementType.TYPE})
public @interface StartUpJob {
    // larger get fired first
    int priority () default 0;
}
