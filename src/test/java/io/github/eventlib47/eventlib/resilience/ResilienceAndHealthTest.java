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
package io.github.eventlib47.eventlib.resilience;

import io.github.eventlib47.eventlib.broker.MessagePublishException;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.eventlib47.eventlib.health.EventLibHealthIndicator;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.boot.actuate.health.Status;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ResilienceAndHealthTest {

    @Test
    void resiliencePolicyRetriesTransientErrorsAndRecordsMetrics() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventLibMetrics metrics = new EventLibMetrics(registry);

        ResilienceProperties props = new ResilienceProperties();
        props.getRetry().setMaxAttempts(3);
        props.getRetry().setBackoff(Duration.ofMillis(10));
        props.getRetry().setMaxBackoff(Duration.ofMillis(20));

        ResiliencePolicy policy = new ResiliencePolicy("pub-test", props, metrics);
        assertNotNull(policy.circuitBreaker());
        assertNotNull(policy.metrics());

        AtomicInteger calls = new AtomicInteger();
        StepVerifier.create(policy.protect(() -> {
                    if (calls.incrementAndGet() < 3) {
                        return Mono.error(new IOException("connection reset"));
                    }
                    return Mono.just("ok");
                }))
                .expectNext("ok")
                .verifyComplete();

        assertEquals(3, calls.get());
        assertEquals(2.0, registry.counter("eventlib.resilience.retries", "name", "pub-test").count());

        // Errores no reintentables fallan en el primer intento
        AtomicInteger nonRetryableCalls = new AtomicInteger();
        StepVerifier.create(policy.protect(() -> {
                    nonRetryableCalls.incrementAndGet();
                    return Mono.error(new MessagePublishException(
                            MessagePublishException.Reason.UNROUTABLE, "unroutable"));
                }))
                .expectError(MessagePublishException.class)
                .verify();
        assertEquals(1, nonRetryableCalls.get());
    }

    @Test
    void metricsRecordsCountersAndGauges() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        EventLibMetrics metrics = new EventLibMetrics(registry);

        metrics.published("ok");
        metrics.outboxTransition("published");
        metrics.outboxPending(7);
        metrics.reconnect("PRODUCER");

        assertEquals(1.0, registry.counter("eventlib.publish", "outcome", "ok").count());
        assertEquals(1.0, registry.counter("eventlib.outbox.transitions", "status", "published").count());
        assertEquals(7.0, registry.get("eventlib.outbox.pending").gauge().value());
        assertEquals(1.0, registry.counter("eventlib.rabbit.reconnects", "type", "PRODUCER").count());
    }

    @Test
    void healthIndicatorReportsUpAndDownBasedOnRedisPing() {
        InfraProperties infra = new InfraProperties();

        // Sin Redis configurado -> UP con detalle not-configured
        EventLibHealthIndicator withoutRedis = new EventLibHealthIndicator(infra, null);
        StepVerifier.create(withoutRedis.health())
                .assertNext(health -> {
                    assertEquals(Status.UP, health.getStatus());
                    assertEquals("not-configured", health.getDetails().get("redis"));
                })
                .verifyComplete();

        // Con Redis respondiendo PONG -> UP
        ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
        ReactiveRedisConnectionFactory factory = mock(ReactiveRedisConnectionFactory.class);
        ReactiveRedisConnection conn = mock(ReactiveRedisConnection.class);
        when(redis.getConnectionFactory()).thenReturn(factory);
        when(factory.getReactiveConnection()).thenReturn(conn);
        when(conn.ping()).thenReturn(Mono.just("PONG"));

        EventLibHealthIndicator healthyRedis = new EventLibHealthIndicator(infra, redis);
        StepVerifier.create(healthyRedis.health())
                .assertNext(health -> {
                    assertEquals(Status.UP, health.getStatus());
                    assertEquals("UP", health.getDetails().get("redis"));
                })
                .verifyComplete();

        // Con Redis fallando -> DOWN
        when(conn.ping()).thenReturn(Mono.error(new RuntimeException("Redis timeout")));
        StepVerifier.create(healthyRedis.health())
                .assertNext(health -> {
                    assertEquals(Status.DOWN, health.getStatus());
                    assertEquals("DOWN", health.getDetails().get("redis"));
                })
                .verifyComplete();
    }
}
