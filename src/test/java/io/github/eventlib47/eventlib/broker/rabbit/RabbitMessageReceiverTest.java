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
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.PermanentEventException;
import io.github.eventlib47.eventlib.consumer.TransientEventException;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.AcknowledgableDelivery;
import reactor.rabbitmq.Receiver;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RabbitMessageReceiverTest {

    private final MessageSerializer serializer =
            new MessageSerializer(new ObjectMapper().findAndRegisterModules());

    @Test
    void acknowledgesSuccessfulDelivery() {
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        when(dispatcher.dispatch(any())).thenReturn(Mono.empty());

        AcknowledgableDelivery delivery = delivery("""
                {"eventName":"test.event","metadata":{"eventId":"evt-1"}}
                """);

        StepVerifier.create(receiver(dispatcher).consumeOne(
                        "test.queue", TestEvent.class, delivery))
                .expectNextCount(1)
                .verifyComplete();

        verify(delivery).ack();
    }

    @Test
    void permanentErrorIsSentToDlqWithoutRequeue() {
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        when(dispatcher.dispatch(any()))
                .thenReturn(Mono.error(new PermanentEventException("invalid")));

        AcknowledgableDelivery delivery = delivery("""
                {"eventName":"test.event","metadata":{"eventId":"evt-1"}}
                """);

        StepVerifier.create(receiver(dispatcher).consumeOne(
                        "test.queue", TestEvent.class, delivery))
                .expectNextCount(1)
                .verifyComplete();

        verify(delivery).nack(false);
    }

    @Test
    void transientErrorIsRequeued() {
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        when(dispatcher.dispatch(any()))
                .thenReturn(Mono.error(new TransientEventException("retry")));

        AcknowledgableDelivery delivery = delivery("""
                {"eventName":"test.event","metadata":{"eventId":"evt-1"}}
                """);

        StepVerifier.create(receiver(dispatcher).consumeOne(
                        "test.queue", TestEvent.class, delivery))
                .expectNextCount(1)
                .verifyComplete();

        verify(delivery).nack(true);
    }

    @Test
    void deserializationErrorIsSentToDlqWithoutRequeue() {
        EventDispatcher dispatcher = mock(EventDispatcher.class);
        AcknowledgableDelivery delivery = delivery("not-json");

        StepVerifier.create(receiver(dispatcher).consumeOne(
                        "test.queue", TestEvent.class, delivery))
                .verifyComplete();

        verify(delivery).nack(false);
    }

    private RabbitMessageReceiver receiver(EventDispatcher dispatcher) {
        return new RabbitMessageReceiver(
                mock(Receiver.class), serializer, dispatcher, Duration.ZERO, 1);
    }

    private AcknowledgableDelivery delivery(String payload) {
        AcknowledgableDelivery delivery = mock(AcknowledgableDelivery.class);
        when(delivery.getBody()).thenReturn(payload.getBytes(StandardCharsets.UTF_8));
        return delivery;
    }
}
