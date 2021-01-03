package io.spring.batch.configuration;

import io.spring.batch.domain.ColumnRangePartitioner;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningManagerStepBuilderFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import javax.sql.DataSource;

@Configuration
@Profile("manager")
@EnableBatchIntegration
public class MasterConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory;
    private final DataSource dataSource;

    public MasterConfiguration(JobBuilderFactory jobBuilderFactory,
                               RemotePartitioningManagerStepBuilderFactory managerStepBuilderFactory, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.managerStepBuilderFactory = managerStepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job remotePartitioningJob(Step masterStep) {
        return jobBuilderFactory.get("remotePartitioningJob").
                incrementer(new RunIdIncrementer()).
                start(masterStep).
                build();
    }

    @Bean
    public Step masterStep(ColumnRangePartitioner partitioner,
                           DirectChannel requests,
                           DirectChannel replies) {
        return managerStepBuilderFactory.get("masterStep").
                partitioner("workerStep", partitioner).
                gridSize(4).
                outputChannel(requests).
                inputChannel(replies).
                build();
    }

    @Bean
    @StepScope
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn("id");
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable("customer");
        return columnRangePartitioner;
    }

    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(DirectChannel requests, AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(requests).
                handle(Amqp.outboundAdapter(amqpTemplate).routingKey("requests")).
                get();
    }

    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(DirectChannel replies, ConnectionFactory connectionFactory) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, "replies")).
                channel(replies).
                get();
    }
}
