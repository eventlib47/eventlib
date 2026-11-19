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
package io.github.eventlib47.eventlib.consumer;

import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.support.RecordingIdempotencyStore;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EventDispatcherTest {

    private static final String OWNER = "consumer-1";
    private static final Duration CLAIM_TTL = Duration.ofSeconds(30);

    @Test
    void processesHandlerAndCompletesClaim() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        AtomicInteger handled = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(store, handler("test.event", event -> {
            handled.incrementAndGet();
            return Mono.empty();
        }));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "test.event")))
                .verifyComplete();

        assertEquals(1, handled.get());
        assertEquals(1, store.claims.get());
        assertEquals(1, store.completions.get());
        assertEquals(0, store.releases.get());
        assertEquals("event-1", store.lastEventId);
        assertTrue(store.lastOwner.startsWith(OWNER + ":"));
        assertEquals(CLAIM_TTL, store.lastTtl);
    }

    @Test
    void completedDuplicateIsAcknowledgedWithoutCallingHandler() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        store.claimResult = IdempotencyStore.ClaimResult.COMPLETED;
        AtomicInteger handled = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(store, handler("test.event", event -> {
            handled.incrementAndGet();
            return Mono.empty();
        }));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "test.event")))
                .verifyComplete();

        assertEquals(0, handled.get());
        assertEquals(0, store.completions.get());
        assertEquals(0, store.releases.get());
    }

    @Test
    void inProgressClaimIsTransientAndDoesNotReleaseOtherOwner() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        store.claimResult = IdempotencyStore.ClaimResult.IN_PROGRESS;
        EventDispatcher dispatcher = dispatcher(store, handler("test.event", event -> Mono.empty()));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "test.event")))
                .expectError(TransientEventException.class)
                .verify();

        assertEquals(0, store.releases.get());
    }

    @Test
    void missingHandlerIsPermanentAndReleasesClaim() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        EventDispatcher dispatcher = dispatcher(store);

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "unknown.event")))
                .expectError(PermanentEventException.class)
                .verify();

        assertEquals(1, store.releases.get());
    }

    @Test
    void handlerFailureReleasesClaim() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        EventDispatcher dispatcher = dispatcher(store, handler("test.event",
                event -> Mono.error(new TransientEventException("db down"))));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "test.event")))
                .expectError(TransientEventException.class)
                .verify();

        assertEquals(1, store.releases.get());
        assertEquals(0, store.completions.get());
    }

    @Test
    void lostClaimWhenCompletingIsTransientAndReleases() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        store.completeResult = false;
        EventDispatcher dispatcher = dispatcher(store, handler("test.event", event -> Mono.empty()));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("event-1", "test.event")))
                .expectError(TransientEventException.class)
                .verify();

        assertEquals(1, store.completions.get());
        assertEquals(1, store.releases.get());
    }

    @Test
    void duplicateHandlersFailFast() {
        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> dispatcher(new RecordingIdempotencyStore(),
                        handler("test.event", event -> Mono.empty()),
                        handler("test.event", event -> Mono.empty())));

        assertEquals(true, error.getMessage().contains("test.event"));
    }

    private EventDispatcher dispatcher(
            RecordingIdempotencyStore store,
            IntegrationEventHandler<?>... handlers) {
        return new EventDispatcher(List.of(handlers), store, OWNER, CLAIM_TTL);
    }

    private IntegrationEventHandler<IntegrationEvent> handler(
            String eventName,
            java.util.function.Function<IntegrationEvent, Mono<Void>> body) {
        return new IntegrationEventHandler<>() {
            @Override
            public Mono<Void> handle(IntegrationEvent event) {
                return body.apply(event);
            }

            @Override
            public String getEventName() {
                return eventName;
            }
        };
    }
}
