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
import io.github.eventlib47.eventlib.broker.MessagePublishException;
import io.github.eventlib47.eventlib.broker.MessagePublishException.Reason;
import io.github.eventlib47.eventlib.broker.rabbit.routing.HashKeyResolver;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.OutboundMessageResult;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RabbitMQPublisherResilienceTest {

    private static final OutboundMessage MESSAGE =
            new OutboundMessage("ex", "rk", null, new byte[0]);
    private static final byte[] BODY = "{}".getBytes(StandardCharsets.UTF_8);

    @Test
    void nackIsRetriedAndThenReportedAsNack() {
        AtomicInteger calls = new AtomicInteger();
        RabbitMQPublisher publisher = publisher(
                calls, () -> Flux.just(new OutboundMessageResult<>(MESSAGE, false)),
                policy(2, Duration.ofSeconds(2)));

        StepVerifier.create(publisher.publish("ex", "rk", BODY))
                .expectErrorSatisfies(error -> {
                    assertTrue(error instanceof MessagePublishException);
                    assertEquals(Reason.NACK, ((MessagePublishException) error).getReason());
                })
                .verify(Duration.ofSeconds(5));

        assertEquals(2, calls.get(), "NACK es reintentable: 2 intentos");
    }

    @Test
    void unroutableMessageIsNeverRetried() {
        AtomicInteger calls = new AtomicInteger();
        RabbitMQPublisher publisher = publisher(
                calls, () -> Flux.just(new OutboundMessageResult<>(MESSAGE, true, true)),
                policy(3, Duration.ofSeconds(2)));

        StepVerifier.create(publisher.publish("ex", "rk", BODY))
                .expectErrorSatisfies(error -> assertEquals(
                        Reason.UNROUTABLE, ((MessagePublishException) error).getReason()))
                .verify(Duration.ofSeconds(5));

        assertEquals(1, calls.get(), "un mensaje no enrutable no se reintenta");
    }

    @Test
    void missingConfirmIsReportedAsTimeout() {
        AtomicInteger calls = new AtomicInteger();
        RabbitMQPublisher publisher = publisher(
                calls, Flux::never, policy(1, Duration.ofMillis(100)));

        StepVerifier.create(publisher.publish("ex", "rk", BODY))
                .expectErrorSatisfies(error -> assertEquals(
                        Reason.TIMEOUT, ((MessagePublishException) error).getReason()))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void emptyConfirmStreamIsReportedAsNoConfirmation() {
        AtomicInteger calls = new AtomicInteger();
        RabbitMQPublisher publisher = publisher(
                calls, Flux::empty, policy(1, Duration.ofSeconds(2)));

        StepVerifier.create(publisher.publish("ex", "rk", BODY))
                .expectErrorSatisfies(error -> assertEquals(
                        Reason.NO_CONFIRMATION, ((MessagePublishException) error).getReason()))
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void ackCompletesNormally() {
        AtomicInteger calls = new AtomicInteger();
        RabbitMQPublisher publisher = publisher(
                calls, () -> Flux.just(new OutboundMessageResult<>(MESSAGE, true)),
                policy(3, Duration.ofSeconds(2)));

        StepVerifier.create(publisher.publish("ex", "rk", BODY)).verifyComplete();
        assertEquals(1, calls.get());
    }

    // ---- helpers ----

    private static ResiliencePolicy policy(int attempts, Duration timeout) {
        ResilienceProperties props = new ResilienceProperties();
        props.getRetry().setMaxAttempts(attempts);
        props.getRetry().setBackoff(Duration.ofMillis(10));
        props.getRetry().setMaxBackoff(Duration.ofMillis(20));
        props.getRetry().setJitter(0);
        props.setTimeout(timeout);
        props.getCircuitBreaker().setEnabled(false);
        props.getBulkhead().setEnabled(false);
        return new ResiliencePolicy("test-publisher", props, EventLibMetrics.noop());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RabbitMQPublisher publisher(
            AtomicInteger calls,
            Supplier<Flux<OutboundMessageResult<OutboundMessage>>> results,
            ResiliencePolicy policy) {
        Sender sender = mock(Sender.class);
        when(sender.sendWithPublishConfirms(any(Publisher.class), any(SendOptions.class)))
                .thenAnswer(invocation -> {
                    calls.incrementAndGet();
                    return results.get();
                });
        return new RabbitMQPublisher(
                sender,
                mock(EventRoutingResolver.class),
                mock(HashKeyResolver.class),
                new ObjectMapper().findAndRegisterModules(),
                "test-service",
                policy);
    }
}
