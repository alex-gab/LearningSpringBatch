package io.spring.batch.launcher;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class ScheduledLauncher {
    private final JobOperator jobOperator;

    public ScheduledLauncher(JobOperator jobOperator) {
        this.jobOperator = jobOperator;
    }

    @Scheduled(fixedDelay = 5000L)
    public void runJob() throws JobExecutionException {
        jobOperator.startNextInstance("job");
    }
}
