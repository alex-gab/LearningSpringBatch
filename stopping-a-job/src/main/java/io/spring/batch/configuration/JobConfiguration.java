package io.spring.batch.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.DefaultBatchConfigurer;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.converter.DefaultJobParametersConverter;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class JobConfiguration extends DefaultBatchConfigurer {
    @Bean
    public JobOperator jobOperator(JobLauncher jobLauncher,
                                   JobRepository jobRepository,
                                   JobExplorer jobExplorer,
                                   JobRegistry jobRegistry) throws Exception {
        SimpleJobOperator simpleJobOperator = new SimpleJobOperator();

        simpleJobOperator.setJobLauncher(jobLauncher);
        simpleJobOperator.setJobParametersConverter(new DefaultJobParametersConverter());
        simpleJobOperator.setJobRepository(jobRepository);
        simpleJobOperator.setJobExplorer(jobExplorer);
        simpleJobOperator.setJobRegistry(jobRegistry);
        simpleJobOperator.afterPropertiesSet();

        return simpleJobOperator;
    }

    @Bean
    public JobRegistryBeanPostProcessor jobRegistrar(JobRegistry jobRegistry) throws Exception {
        JobRegistryBeanPostProcessor registrar = new JobRegistryBeanPostProcessor();
        registrar.setJobRegistry(jobRegistry);
        registrar.afterPropertiesSet();
        return registrar;
    }

    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get("job").
                start(step).
                build();
    }

    @Bean
    public Step step(StepBuilderFactory stepBuilderFactory, Tasklet tasklet) {
        return stepBuilderFactory.get("step").
                tasklet(tasklet).
                build();
    }

    @Bean
    @StepScope
    public Tasklet tasklet(@Value("#{jobParameters['name']}") String name) {
        return (contribution, chunkContext) -> {
            System.out.printf(">> %s is sleeping again...%n", name);
            Thread.sleep(1000);
            return RepeatStatus.CONTINUABLE;
        };
    }

    @Override
    public JobLauncher getJobLauncher() {
        SimpleJobLauncher jobLauncher;
        try {
            jobLauncher = new SimpleJobLauncher();
            jobLauncher.setJobRepository(getJobRepository());
            jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor());
            jobLauncher.afterPropertiesSet();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return jobLauncher;
    }
}
