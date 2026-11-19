/*
 * Licensed to Elkim Andres Medranda Caicedo under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * Elkim Andres Medranda Caicedo licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.eventlib47.eventlib.autoconfigure;

import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.EventPublisher;
import io.github.eventlib47.eventlib.producer.OutboxEventPublisher;
import io.github.eventlib47.eventlib.producer.outbox.OutboxDistributedLock;
import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import io.github.eventlib47.eventlib.producer.outbox.OutboxRepository;
import io.github.eventlib47.eventlib.producer.outbox.OutboxScheduler;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.reactive.TransactionalOperator;

import java.util.UUID;

/**
 * Auto-configuración del Transactional Outbox (solo con
 * {@code eventlib.infra.outbox.enabled=true}).
 */
@AutoConfiguration(
        after = {EventLibAutoConfiguration.class,
                RabbitAutoConfiguration.class,
                RedisAutoConfiguration.class},
        afterName = {
                "org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration",
                "org.springframework.boot.autoconfigure.r2dbc.R2dbcTransactionManagerAutoConfiguration",
                "org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration"})
@ConditionalOnProperty(prefix = "eventlib.infra.outbox", name = "enabled", havingValue = "true")
@ConditionalOnBean({MessagePublisher.class, MessageSerializer.class})
@EnableScheduling
public class OutboxAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(OutboxRepository.class)
    @EnableR2dbcRepositories(basePackageClasses = OutboxRepository.class)
    static class OutboxRepositoryConfiguration {
    }

    @Bean
    public OutboxProcessor outboxProcessor(
            OutboxRepository repository,
            @Lazy MessagePublisher publisher,
            @Lazy MessageSerializer serializer,
            EventRoutingResolver routingResolver,
            InfraProperties infraProperties,
            DatabaseClient databaseClient,
            EventLibMetrics metrics,
            @Value("${spring.application.name:unknown-service}") String appName
    ) {
        String owner = appName + ":" + UUID.randomUUID();
        return new OutboxProcessor(
                repository,
                publisher,
                serializer,
                routingResolver,
                infraProperties,
                databaseClient,
                owner,
                metrics);
    }

    @Bean
    @ConditionalOnBean(ReactiveStringRedisTemplate.class)
    @ConditionalOnProperty(prefix = "eventlib.infra.outbox", name = "lock-enabled",
            havingValue = "true", matchIfMissing = true)
    public OutboxDistributedLock outboxDistributedLock(
            ReactiveStringRedisTemplate redis,
            InfraProperties infraProperties,
            @Value("${spring.application.name:unknown-service}") String appName) {
        return new OutboxDistributedLock(redis, infraProperties, appName);
    }

    @Bean
    public OutboxScheduler outboxScheduler(
            OutboxRepository repository,
            OutboxProcessor processor,
            InfraProperties infraProperties,
            ObjectProvider<OutboxDistributedLock> lockProvider,
            EventLibMetrics metrics) {
        return new OutboxScheduler(
                repository,
                processor,
                infraProperties,
                lockProvider.getIfAvailable(),
                metrics,
                infraProperties.getResilience().getShutdownTimeout());
    }

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    public OutboxEventPublisher eventPublisher(
            OutboxProcessor outboxProcessor,
            ObjectProvider<TransactionalOperator> transactionalOperator) {
        return new OutboxEventPublisher(
                outboxProcessor, transactionalOperator.getIfAvailable());
    }
}
