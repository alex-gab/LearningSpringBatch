package io.spring.batch.configuration;

import io.spring.batch.domain.Customer;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;

import javax.sql.DataSource;

@Configuration
@Profile("worker")
@EnableBatchIntegration
public class WorkerConfiguration {
    private final RemoteChunkingWorkerBuilder<Customer, Customer> workerBuilder;
    private final DataSource dataSource;

    public WorkerConfiguration(RemoteChunkingWorkerBuilder<Customer, Customer> workerBuilder, DataSource dataSource) {
        this.workerBuilder = workerBuilder;
        this.dataSource = dataSource;
    }

    @Bean
    public IntegrationFlow integrationFlow(ItemProcessor<Customer, Customer> processor,
                                           JdbcBatchItemWriter<Customer> customerItemWriter,
                                           DirectChannel requests,
                                           DirectChannel replies) {
        return workerBuilder.itemProcessor(processor).
                itemWriter(customerItemWriter).
                inputChannel(requests).
                outputChannel(replies).
                build();
    }

    @Bean
    public ItemProcessor<Customer, Customer> processor() {
        return customer -> new Customer(
                customer.getId(),
                customer.getFirstName().toUpperCase(),
                customer.getLastName().toUpperCase(),
                customer.getBirthdate());
    }

    @Bean
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
