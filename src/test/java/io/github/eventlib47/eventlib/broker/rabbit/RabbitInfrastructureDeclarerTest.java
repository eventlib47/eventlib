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
package io.github.eventlib47.eventlib.broker.rabbit;

import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Queue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class RabbitInfrastructureDeclarerTest {

    @Test
    void appliesQuorumLimitsTtlAndDeadLetterArguments() {
        RabbitProperties properties = new RabbitProperties();
        properties.setQueueType("quorum");
        properties.setMaxQueueLength(1_000);
        properties.setMessageTtl(Duration.ofSeconds(30));

        EventRoute route = new EventRoute(
                "user-created",
                "auth.user.v1.created",
                "auth.exchange",
                "auth.user.v1.created",
                "email.user.v1.created.queue",
                RabbitProperties.ExchangeType.TOPIC,
                1,
                true,
                null);

        RabbitInfrastructureDeclarer declarer = new RabbitInfrastructureDeclarer(
                resolver(route), properties);

        Queue queue = queueNamed(declarer.declarables(), "email.user.v1.created.queue");
        Queue dlq = queueNamed(
                declarer.declarables(), "email.user.v1.created.queue.dlq");

        assertNotNull(queue);
        assertEquals("quorum", queue.getArguments().get("x-queue-type"));
        assertEquals(1_000L, queue.getArguments().get("x-max-length")); // <-- Cambiado a 1_000L
        assertEquals("reject-publish", queue.getArguments().get("x-overflow"));
        assertEquals(30_000L, queue.getArguments().get("x-message-ttl"));
        assertEquals("auth.exchange",
                queue.getArguments().get("x-dead-letter-exchange"));
        assertEquals("auth.user.v1.created.dlq",
                queue.getArguments().get("x-dead-letter-routing-key"));
        assertEquals("quorum", dlq.getArguments().get("x-queue-type"));
    }

    private Queue queueNamed(List<Declarable> declarables, String name) {
        return declarables.stream()
                .filter(Queue.class::isInstance)
                .map(Queue.class::cast)
                .filter(queue -> name.equals(queue.getName()))
                .findFirst()
                .orElseThrow();
    }

    private EventRoutingResolver resolver(EventRoute route) {
        return new EventRoutingResolver() {
            @Override
            public EventRoute resolve(IntegrationEvent event) {
                return route;
            }

            @Override
            public EventRoute resolve(String eventName) {
                return route;
            }

            @Override
            public Collection<EventRoute> resolveAll() {
                return List.of(route);
            }

            @Override
            public String defaultExchange() {
                return "eventlib.exchange";
            }
        };
    }
}