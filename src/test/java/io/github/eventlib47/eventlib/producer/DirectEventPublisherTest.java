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
package io.github.eventlib47.eventlib.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DirectEventPublisherTest {

    private MessagePublisher messagePublisher;
    private EventRoutingResolver routingResolver;
    private DirectEventPublisher publisher;

    @BeforeEach
    void setUp() {
        messagePublisher = mock(MessagePublisher.class);
        routingResolver = mock(EventRoutingResolver.class);
        publisher = new DirectEventPublisher(
                messagePublisher,
                new MessageSerializer(new ObjectMapper()),
                routingResolver);
    }

    @Test
    void rejectsNullEvent() {
        StepVerifier.create(publisher.publish(null))
                .expectError(IllegalArgumentException.class)
                .verify();
    }

    @Test
    void failsWhenRouteIsNull() {
        TestEvent event = new TestEvent("evt-1", "order.created");
        when(routingResolver.resolve(event)).thenReturn(null);

        StepVerifier.create(publisher.publish(event))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void publishesToTopicExchangeWithResolvedRoutingKey() {
        TestEvent event = new TestEvent("evt-1", "order.created");
        EventRoute route = new EventRoute(
                "order", "order.created", "orders.ex", "order.rk",
                "orders.q", RabbitProperties.ExchangeType.TOPIC, 1, true, null);

        when(routingResolver.resolve(event)).thenReturn(route);
        when(messagePublisher.publish(eq("orders.ex"), eq("order.rk"), any(byte[].class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(publisher.publish(event)).verifyComplete();

        verify(messagePublisher).publish(eq("orders.ex"), eq("order.rk"), any(byte[].class));
    }

    @Test
    void usesAggregateIdAsRoutingKeyForConsistentHashExchange() {
        TestEvent event = new TestEvent("evt-99", "order.created");
        event.getMetadata().setAggregateId("agg-42");
        EventRoute route = new EventRoute(
                "order", "order.created", "orders.hash.ex", "ignored.rk",
                "orders.q", RabbitProperties.ExchangeType.X_CONSISTENT_HASH, 4, true, null);

        when(routingResolver.resolve(event)).thenReturn(route);
        when(messagePublisher.publish(eq("orders.hash.ex"), eq("agg-42"), any(byte[].class)))
                .thenReturn(Mono.empty());

        StepVerifier.create(publisher.publish(event)).verifyComplete();

        verify(messagePublisher).publish(eq("orders.hash.ex"), eq("agg-42"), any(byte[].class));
    }
}
