package io.spring.batch.configuration;

import io.spring.batch.domain.Customer;
import io.spring.batch.domain.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class MultithreadedStepJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    public MultithreadedStepJobConfiguration(JobBuilderFactory jobBuilderFactory,
                                             StepBuilderFactory stepBuilderFactory,
                                             DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job multithreadedStepJob(Step multithreadedStep) {
        return jobBuilderFactory.get("multithreadedStepJob").
                start(multithreadedStep).
                build();
    }

    @Bean
    public Step multithreadedStep(JdbcPagingItemReader<Customer> pagingItemReader,
                                  JdbcBatchItemWriter<Customer> customerItemWriter) {
        return stepBuilderFactory.get("multithreadedStep").
                <Customer, Customer>chunk(1000).
                reader(pagingItemReader).
                writer(customerItemWriter).
                taskExecutor(new SimpleAsyncTaskExecutor()).
                build();
    }

    @Bean
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
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>().
                dataSource(dataSource).
                sql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)").
                itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()).
                build();
    }

}
