package io.spring.batch.configuration;

import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.support.ListItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.gateway.GatewayProxyFactoryBean;
import org.springframework.integration.stream.CharacterStreamWritingMessageHandler;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class JobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final ApplicationContext applicationContext;

    public JobConfiguration(JobBuilderFactory jobBuilderFactory,
                            StepBuilderFactory stepBuilderFactory,
                            ApplicationContext applicationContext) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.applicationContext = applicationContext;
    }

    @Bean
    public Job job(Step step,
                   @Qualifier("jobExecutionListener") Object jobExecutionListener) {
        return jobBuilderFactory.get("job").
                start(step).
                listener((JobExecutionListener) jobExecutionListener).
                build();
    }

    @Bean
    public Step step(ItemReader<String> itemReader,
                     ItemWriter<String> itemWriter,
                     @Qualifier("chunkListener") Object chunkListener) {
        return stepBuilderFactory.get("step").
                <String, String>chunk(100).
                reader(itemReader).
                writer(itemWriter).
                listener((ChunkListener) chunkListener).
                build();
    }

    @Bean
    @StepScope
    public ItemReader<String> itemReader() {
        List<String> items = new ArrayList<>(1000);

        for (int i = 0; i < 1000; i++) {
            items.add(String.valueOf(i));
        }

        return new ListItemReader<>(items);
    }

    @Bean
    @StepScope
    public ItemWriter<String> itemWriter() {
        return items -> {
            for (String item : items) {
                System.out.println(">> " + item);
            }
        };
    }

    @Bean
    public DirectChannel events() {
        return new DirectChannel();
    }

    @Bean
    public Object jobExecutionListener(DirectChannel events) {
        GatewayProxyFactoryBean proxyFactoryBean = new GatewayProxyFactoryBean(JobExecutionListener.class);

        proxyFactoryBean.setDefaultRequestChannel(events);
        proxyFactoryBean.setBeanFactory(this.applicationContext);

        return proxyFactoryBean.getObject();
    }

    @Bean
    public Object chunkListener(DirectChannel events) {
        GatewayProxyFactoryBean proxyFactoryBean = new GatewayProxyFactoryBean(ChunkListener.class);

        proxyFactoryBean.setDefaultRequestChannel(events);
        proxyFactoryBean.setBeanFactory(this.applicationContext);

        return proxyFactoryBean.getObject();
    }

    @Bean
    @ServiceActivator(inputChannel = "events")
    public CharacterStreamWritingMessageHandler logger() {
        return CharacterStreamWritingMessageHandler.stdout();
    }
}
