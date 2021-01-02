package io.spring.batch.controller;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.http.HttpStatus;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.*;

@RestController
public class JobLaunchingController {
    private final Job job;
    private final DirectChannel requests;
    private final DirectChannel replies;

    public JobLaunchingController(Job job, DirectChannel requests, DirectChannel replies) {
        this.job = job;
        this.requests = requests;
        this.replies = replies;
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    @ResponseStatus(HttpStatus.ACCEPTED)
    public void launch(@RequestParam("name") String name) {
        JobParameters jobParameters = new JobParametersBuilder().
                addString("name", name).
                toJobParameters();
        JobLaunchRequest launchRequest = new JobLaunchRequest(job, jobParameters);

        replies.subscribe(message -> {
            JobExecution payload = (JobExecution) message.getPayload();
            System.out.println(">> " + payload.getJobInstance().getJobName() + " resulted in " + payload.getStatus());
        });
        requests.send(
                MessageBuilder.withPayload(launchRequest).
                        setReplyChannel(replies).
                        build());
    }
}
