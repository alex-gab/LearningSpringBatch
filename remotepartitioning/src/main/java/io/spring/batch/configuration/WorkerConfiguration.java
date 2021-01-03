package io.spring.batch.configuration;

import io.spring.batch.domain.Customer;
import io.spring.batch.domain.CustomerRowMapper;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.integration.partition.RemotePartitioningWorkerStepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Profile("!manager")
@EnableBatchIntegration
public class WorkerConfiguration {
    private final RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory;
    private final DataSource dataSource;

    public WorkerConfiguration(RemotePartitioningWorkerStepBuilderFactory workerStepBuilderFactory, DataSource dataSource) {
        this.workerStepBuilderFactory = workerStepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Step workerStep(DirectChannel requests,
                           DirectChannel replies,
                           JdbcPagingItemReader<Customer> pagingItemReader,
                           JdbcBatchItemWriter<Customer> customerItemWriter) {
        return workerStepBuilderFactory.get("workerStep").
                inputChannel(requests).
                outputChannel(replies).
                <Customer, Customer>chunk(1000).
                reader(pagingItemReader).
                writer(customerItemWriter).
                build();
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Customer> pagingItemReader(@Value("#{stepExecutionContext['minValue']}") Long minValue,
                                                           @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println("reading " + minValue + " to " + maxValue);
        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
        queryProvider.setSelectClause("id, firstName, lastName, birthdate");
        queryProvider.setFromClause("from customer");
        queryProvider.setWhereClause("where id >= " + minValue + " and id < " + maxValue);

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
    @StepScope
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>().
                dataSource(dataSource).
                sql("INSERT INTO new_customer VALUES (:id, :firstName, :lastName, :birthdate)").
                itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()).
                build();
    }

    @Bean
    public DirectChannel requests() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow inboundFlow(DirectChannel requests, ConnectionFactory connectionFactory) {
        return IntegrationFlows.from(Amqp.inboundAdapter(connectionFactory, "requests")).
                channel(requests).
                get();
    }

    @Bean
    public DirectChannel replies() {
        return new DirectChannel();
    }

    @Bean
    public IntegrationFlow outboundFlow(DirectChannel replies, AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(replies).
                handle(Amqp.outboundAdapter(amqpTemplate).routingKey("replies")).
                get();
    }
}
