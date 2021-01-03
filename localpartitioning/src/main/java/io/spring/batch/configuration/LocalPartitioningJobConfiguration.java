package io.spring.batch.configuration;

import io.spring.batch.domain.ColumnRangePartitioner;
import io.spring.batch.domain.Customer;
import io.spring.batch.domain.CustomerRowMapper;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.database.support.H2PagingQueryProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class LocalPartitioningJobConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final DataSource dataSource;

    public LocalPartitioningJobConfiguration(JobBuilderFactory jobBuilderFactory,
                                             StepBuilderFactory stepBuilderFactory,
                                             DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.dataSource = dataSource;
    }

    @Bean
    public Job job(Step masterStep) {
        return jobBuilderFactory.get("localPartitioningJob").
                start(masterStep).
                build();
    }

    @Bean
    public Step masterStep(Step slaveStep, ColumnRangePartitioner partitioner) {
        return stepBuilderFactory.get("masterStep").
                partitioner(slaveStep.getName(), partitioner).
                step(slaveStep).
                gridSize(4).
                taskExecutor(new SimpleAsyncTaskExecutor()).
                build();
    }

    @Bean
    public Step slaveStep(JdbcPagingItemReader<Customer> pagingItemReader,
                          JdbcBatchItemWriter<Customer> customerItemWriter) {
        return stepBuilderFactory.get("slaveStep").
                <Customer, Customer>chunk(1000).
                reader(pagingItemReader).
                writer(customerItemWriter).
                build();
    }

    @Bean
    public ColumnRangePartitioner partitioner() {
        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
        columnRangePartitioner.setColumn("id");
        columnRangePartitioner.setDataSource(dataSource);
        columnRangePartitioner.setTable("customer");
        return columnRangePartitioner;
    }

    @Bean
    @StepScope
    public JdbcPagingItemReader<Customer> pagingItemReader(@Value("#{stepExecutionContext['minValue']}") Long minValue,
                                                           @Value("#{stepExecutionContext['maxValue']}") Long maxValue) {
        System.out.println("reading " + minValue + " to " + maxValue);
        H2PagingQueryProvider queryProvider = new H2PagingQueryProvider();
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
    public JdbcBatchItemWriter<Customer> customerItemWriter() {
        return new JdbcBatchItemWriterBuilder<Customer>().
                dataSource(dataSource).
                sql("INSERT INTO NEW_CUSTOMER VALUES (:id, :firstName, :lastName, :birthdate)").
                itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>()).
                build();
    }
}
