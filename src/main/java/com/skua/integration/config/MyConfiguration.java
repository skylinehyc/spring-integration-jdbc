package com.skua.integration.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.store.PriorityCapableChannelMessageStore;
import org.springframework.messaging.MessageChannel;

import java.util.concurrent.atomic.AtomicInteger;

@Configuration
@EnableIntegration
public class MyConfiguration {

    @Bean
    public AtomicInteger integerSource() {
        return new AtomicInteger();
    }

    @Bean
    public MessageChannel queueChannel() {
        return MessageChannels.queue().get();
    }
    @Bean
    public MessageChannel publishSubscribe() {
        return MessageChannels.publishSubscribe().get();
    }


    @Bean
    public IntegrationFlow integerFlow() {
        return IntegrationFlows.from("input")
                .filter("World"::equals)
                .<String, Integer>transform(Integer::parseInt)
                .handle(System.out::println)
                .get();
    }
}
