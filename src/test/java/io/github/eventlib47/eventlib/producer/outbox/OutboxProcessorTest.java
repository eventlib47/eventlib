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
package io.github.eventlib47.eventlib.producer.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxProcessorTest {

    @Test
    void storesCompleteRouteInsideTheBusinessTransaction() {
        OutboxRepository repository = mock(OutboxRepository.class);
        when(repository.save(any(OutboxEvent.class)))
                .thenAnswer(invocation -> Mono.just(invocation.getArgument(0)));

        OutboxProcessor processor = processor(repository, mock(MessagePublisher.class));
        TestEvent event = new TestEvent("evt-1", "test.event");

        StepVerifier.create(processor.store(event)).verifyComplete();

        ArgumentCaptor<OutboxEvent> captor = ArgumentCaptor.forClass(OutboxEvent.class);
        verify(repository).save(captor.capture());
        OutboxEvent stored = captor.getValue();

        assertEquals("evt-1", stored.getEventId());
        assertEquals("test.event", stored.getEventName());
        assertEquals("auth.exchange", stored.getExchange());
        assertEquals("test.event", stored.getRoutingKey());
        assertEquals(OutboxEvent.STATUS_PENDING, stored.getStatus());
        assertEquals(0, stored.getAttempts());
    }

    private OutboxProcessor processor(
            OutboxRepository repository,
            MessagePublisher publisher) {
        return new OutboxProcessor(
                repository,
                publisher,
                new MessageSerializer(new ObjectMapper()),
                resolver(),
                noTxInfra(),
                mock(DatabaseClient.class),
                "owner-1");
    }

    private InfraProperties noTxInfra() {
        InfraProperties infra = new InfraProperties();
        infra.getOutbox().setRequireTransaction(false);
        return infra;
    }

    private EventRoutingResolver resolver() {
        EventRoute route = new EventRoute(
                "test",
                "test.event",
                "auth.exchange",
                "test.event",
                "test.queue",
                RabbitProperties.ExchangeType.TOPIC,
                1,
                true,
                null);

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