package io.spring.batch;

import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
public class JobLaunchingController {
    private final JobOperator jobOperator;

    public JobLaunchingController(JobOperator jobOperator) {
        this.jobOperator = jobOperator;
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public long launch(@RequestParam("name") String name) throws JobExecutionException {
        return jobOperator.start("job", String.format("name=%s", name));
    }

    @RequestMapping(value = "/{id}", method = RequestMethod.DELETE)
    @ResponseStatus(HttpStatus.OK)
    public void stop(@PathVariable("id") long id) throws JobExecutionException {
        jobOperator.stop(id);
    }
}
