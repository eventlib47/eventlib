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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitMessageReceiver;
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.IntegrationEventHandler;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore;
import io.github.eventlib47.eventlib.consumer.idempotency.ReactiveRedisIdempotencyStore;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.EventPublisher;
import io.github.eventlib47.eventlib.producer.OutboxEventPublisher;
import io.github.eventlib47.eventlib.producer.outbox.OutboxDistributedLock;
import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import io.github.eventlib47.eventlib.producer.outbox.OutboxRepository;
import io.github.eventlib47.eventlib.producer.outbox.OutboxScheduler;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.Receiver;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class EventLibAutoConfigurationsTest {

    @Test
    void baseAutoConfigurationRegistersSerializerMetricsAndHealthIndicator() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(EventLibAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .run(context -> {
                    assertThat(context).hasSingleBean(MessageSerializer.class);
                    assertThat(context).hasSingleBean(EventLibMetrics.class);
                    assertThat(context).hasBean("eventLibHealthIndicator");
                    assertThat(context).hasSingleBean(ReactiveHealthIndicator.class);
                });
    }

    @Test
    void redisAutoConfigurationRegistersReactiveStoreOrNoOpStore() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        EventLibAutoConfiguration.class,
                        RedisAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(ReactiveRedisConnectionFactory.class, () -> mock(ReactiveRedisConnectionFactory.class))
                .withBean(org.springframework.data.redis.connection.RedisConnectionFactory.class,
                        () -> mock(org.springframework.data.redis.connection.RedisConnectionFactory.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(ReactiveStringRedisTemplate.class);
                    assertThat(context).hasSingleBean(ReactiveRedisIdempotencyStore.class);
                });

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        EventLibAutoConfiguration.class,
                        RedisAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withPropertyValues(
                        "eventlib.infra.redis.enabled=false",
                        "eventlib.infra.redis.allow-no-op=true")
                .run(context -> assertThat(context).hasSingleBean(IdempotencyStore.class));
    }

    @Test
    void consumerAutoConfigurationRegistersDispatcherAndReceiverWhenHandlerExists() {
        IntegrationEventHandler<TestEvent> handler = new IntegrationEventHandler<>() {
            @Override
            public Mono<Void> handle(TestEvent event) {
                return Mono.empty();
            }

            @Override
            public String getEventName() {
                return "test.event";
            }
        };

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        EventLibAutoConfiguration.class,
                        ConsumerAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(IntegrationEventHandler.class, () -> handler)
                .withBean(IdempotencyStore.class, () -> mock(IdempotencyStore.class))
                .withBean(Receiver.class, () -> mock(Receiver.class))
                .run(context -> {
                    assertThat(context).hasSingleBean(EventDispatcher.class);
                    assertThat(context).hasSingleBean(RabbitMessageReceiver.class);
                });
    }

    @Test
    void outboxAutoConfigurationRegistersOutboxBeansWhenEnabled() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        EventLibAutoConfiguration.class,
                        OutboxAutoConfiguration.class))
                .withBean(ObjectMapper.class, ObjectMapper::new)
                .withBean(MessagePublisher.class, () -> mock(MessagePublisher.class))
                .withBean(EventRoutingResolver.class, () -> mock(EventRoutingResolver.class))
                .withBean(OutboxRepository.class, () -> mock(OutboxRepository.class))
                .withBean(DatabaseClient.class, () -> mock(DatabaseClient.class))
                .withBean(ReactiveStringRedisTemplate.class, () -> mock(ReactiveStringRedisTemplate.class))
                .withPropertyValues("eventlib.infra.outbox.enabled=true")
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxProcessor.class);
                    assertThat(context).hasSingleBean(OutboxDistributedLock.class);
                    assertThat(context).hasSingleBean(OutboxScheduler.class);
                    assertThat(context).hasSingleBean(OutboxEventPublisher.class);
                    assertThat(context).hasSingleBean(EventPublisher.class);
                });
    }
}
