package com.skua.integration.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Splitter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.messaging.MessageHandler;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.google.common.base.Splitter.on;

@Configuration
public class BatchExecutorConfig {
    private static final Logger log = LoggerFactory.getLogger(BatchExecutorConfig.class);

    @Value("${sql.insert}")
    private String sql;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @Value("${file.pollInterval}")
    private int pollInterval;

    @Value("${file.path}")
    private String filePath;

    @Value("${file.name}")
    private String fileName;

    @Value("${batch.size}")
    private int batchSize;

    private List<String> headers;

    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;

    @PostConstruct
    public void postSetup() {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
    }

    @Bean
    public IntegrationFlow batchExecutionFlow() {
        return IntegrationFlows.from(
                s -> s.file(new File(filePath))
                        .patternFilter(fileName)
                , e -> e.autoStartup(true).poller(Pollers.fixedDelay(pollInterval)))
                .handle(this, "readCsvFile")
                .channel(csvPayloadChannel())
                .get();
    }

    @ServiceActivator
    public List<List<String>> readCsvFile(File file) {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            headers = reader.lines()
                    .findFirst()
                    .map(line -> on(",").splitToList(line))
                    .get();

            return reader.lines()
                    .map(line -> on(",").splitToList(line))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Bean
    public DirectChannel csvPayloadChannel() {
        return MessageChannels.direct().get();
    }

    @Bean
    public IntegrationFlow flowBatchExec() {
        return IntegrationFlows.from("csvPayloadChannel")
                .split(this, "splitPayloadIntoBatch")
                .channel(MessageChannels.executor(Executors.newFixedThreadPool(4)))
                .handle(this.saveBatchToDb())
                .get();
    }

    @Splitter
    public List<List<List<String>>> splitPayloadIntoBatch(List<List<String>> sourceList) {
        List<List<List<String>>> payloads = new ArrayList<>(sourceList.size() / batchSize + 1);
        for (int i = 0; i <= sourceList.size() / batchSize; i++) {
            payloads.add(sourceList.subList(i * batchSize, (sourceList.size() - (i * batchSize)) < batchSize ? sourceList.size() : (i + 1) * batchSize - 1));
        }
        return payloads;
    }

    @Bean
    public MessageHandler saveBatchToDb() {
        return message -> {
            log.info("Received batch with header %s", message.getHeaders());
            List<List<String>> records = (List<List<String>>) message.getPayload();
            List<MapSqlParameterSource> sqlParameterSources = new ArrayList<>(records.size());
            records.stream()
                    .filter(line -> line.size() == headers.size())
                    .forEach(line -> {
                        int columnNum = 0;
                        MapSqlParameterSource sqlParameterSource = new MapSqlParameterSource();
                        for (String column : line) {
                            sqlParameterSource.addValue(headers.get(columnNum), column);
                            columnNum++;
                        }
                        sqlParameterSources.add(sqlParameterSource);
                    });

            transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                protected void doInTransactionWithoutResult(TransactionStatus status) {
                    namedParameterJdbcTemplate.batchUpdate(sql, sqlParameterSources.toArray(new MapSqlParameterSource[0]));
                }
            });
            log.info("Finished batch with header %s", message.getHeaders());
        };
    }
}
