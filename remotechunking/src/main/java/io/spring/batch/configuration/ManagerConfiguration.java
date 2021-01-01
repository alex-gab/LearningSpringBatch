package io.spring.batch.configuration;

import io.spring.batch.domain.Customer;
import io.spring.batch.domain.CustomerRowMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!worker")
@EnableBatchIntegration
public class ManagerConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory;
    private final DataSource dataSource;

    public ManagerConfiguration(JobBuilderFactory jobBuilderFactory,
                                RemoteChunkingManagerStepBuilderFactory remoteChunkingManagerStepBuilderFactory,
                                DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.remoteChunkingManagerStepBuilderFactory = remoteChunkingManagerStepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job remoteChunkingJob(Step managerStep) {
        return jobBuilderFactory.get("remoteChunkingJob").
                incrementer(new RunIdIncrementer()).
                start(managerStep).
                build();
    }

    @Bean
    public Step managerStep(JdbcPagingItemReader<Customer> pagingItemReader,
                            DirectChannel requests,
                            QueueChannel replies) {
        return remoteChunkingManagerStepBuilderFactory.get("managerStep").
                chunk(1000).
                reader(pagingItemReader).
                outputChannel(requests).
                inputChannel(replies).
                build();
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Customer> pagingItemReader() {
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, birthdate");
        queryProvider.setFromClause("from customer");

        Map<String, Order> sortKeys = new HashMap<>(1);

        sortKeys.put("id", Order.ASCENDING);

        queryProvider.setSortKeys(sortKeys);

        return new JdbcPagingItemReaderBuilder<Customer>().
                dataSource(dataSource).
                fetchSize(1000).
                rowMapper(new CustomerRowMapper()).
                queryProvider(queryProvider).
                saveState(false).
                build();
    }

    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(DirectChannel requests, AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(requests).
                handle(Amqp.outboundAdapter(amqpTemplate).
                        routingKey("requests")).
                get();
    }

    @Bean
    public QueueChannel replies() {
        return new QueueChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(QueueChannel replies, ConnectionFactory connectionFactory) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, "replies")).
                channel(replies).
                get();
    }
}
