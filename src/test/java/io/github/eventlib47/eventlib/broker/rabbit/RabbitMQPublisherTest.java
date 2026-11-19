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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.broker.rabbit.routing.HashKeyResolver;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RabbitMQPublisherTest {

    @Test
    void publishesPersistentMessageWithEventMetadata() {
        Sender sender = mock(Sender.class);
        AtomicReference<OutboundMessage> captured = new AtomicReference<>();

        when(sender.sendWithPublishConfirms(
                any(Publisher.class), any(SendOptions.class)))
                .thenAnswer(invocation -> {
                    Publisher<OutboundMessage> publisher = invocation.getArgument(0);
                    OutboundMessage message = Mono.from(publisher)
                            .block(Duration.ofSeconds(1));
                    captured.set(message);
                    return Flux.just(new OutboundMessageResult<>(message, true));
                });

        RabbitMQPublisher publisher = new RabbitMQPublisher(
                sender,
                resolver(),
                mock(HashKeyResolver.class),
                new ObjectMapper().findAndRegisterModules(),
                "auth-service",
                new RabbitProperties());

        String body = """
                {"eventName":"auth.user.v1.created","metadata":{"eventId":"evt-1"}}
                """;

        StepVerifier.create(publisher.publish(
                        "auth.exchange",
                        "auth.user.v1.created",
                        body.getBytes(StandardCharsets.UTF_8)))
                .verifyComplete();

        OutboundMessage message = captured.get();
        assertEquals("auth.exchange", message.getExchange());
        assertEquals("auth.user.v1.created", message.getRoutingKey());
        assertEquals(2, message.getProperties().getDeliveryMode());
        assertEquals("application/json", message.getProperties().getContentType());
        assertEquals("auth-service", message.getProperties().getAppId());
        assertEquals("evt-1", message.getProperties().getMessageId());
        assertEquals("evt-1", message.getProperties().getCorrelationId());
        assertEquals("auth.user.v1.created", message.getProperties().getType());
    }

    private EventRoutingResolver resolver() {
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
            public java.util.Collection<EventRoute> resolveAll() {
                return java.util.List.of(route);
            }

            @Override
            public String defaultExchange() {
                return "eventlib.exchange";
            }
        };
    }
}
