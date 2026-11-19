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
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EventDispatcherConcurrencyTest {

    @Test
    void twoSimultaneousDeliveriesOfTheSameEventRunTheHandlerOnce() {
        ExpiringStore store = new ExpiringStore();
        AtomicInteger runs = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(
                store, Duration.ofSeconds(5), handler(runs, new AtomicInteger(), Duration.ofMillis(400)));
        TestEvent event = new TestEvent("evt-1", "test.event");

        Mono<Object> first = outcome(dispatcher.dispatch(event));
        Mono<Object> second = outcome(dispatcher.dispatch(event));

        StepVerifier.create(Mono.zip(first, second))
                .assertNext(tuple -> {
                    List<Object> results = List.of(tuple.getT1(), tuple.getT2());
                    assertEquals(1, results.stream().filter("OK"::equals).count());
                    assertEquals(1, results.stream()
                            .filter(r -> r instanceof TransientEventException).count());
                })
                .verifyComplete();

        assertEquals(1, runs.get(), "el handler debe ejecutarse exactamente una vez");
        assertEquals(1, store.completions.get());
    }

    @Test
    void heartbeatKeepsTheClaimAliveBeyondItsTtl() {
        ExpiringStore store = new ExpiringStore();
        AtomicInteger runs = new AtomicInteger();
        // TTL 300 ms, handler 900 ms: sin heartbeat el claim expiraría a los 300 ms.
        EventDispatcher dispatcher = dispatcher(
                store, Duration.ofMillis(300),
                handler(runs, new AtomicInteger(), Duration.ofMillis(900)));
        TestEvent event = new TestEvent("evt-2", "test.event");

        Mono<Object> first = outcome(dispatcher.dispatch(event));
        // Segunda entrega a los 600 ms (> TTL): debe seguir viendo IN_PROGRESS.
        Mono<Object> second = Mono.delay(Duration.ofMillis(600))
                .then(outcome(dispatcher.dispatch(event)));

        StepVerifier.create(Mono.zip(first, second))
                .assertNext(tuple -> {
                    assertEquals("OK", tuple.getT1());
                    assertTrue(tuple.getT2() instanceof TransientEventException);
                })
                .verifyComplete();

        assertEquals(1, runs.get());
        assertTrue(store.renewals.get() >= 2, "el heartbeat debió renovar el claim");
    }

    @Test
    void lostClaimCancelsTheHandlerAndNeverMarksCompleted() {
        ExpiringStore store = new ExpiringStore();
        store.rejectRenewals = true;
        AtomicInteger cancelled = new AtomicInteger();
        EventDispatcher dispatcher = dispatcher(
                store, Duration.ofMillis(150),
                handler(new AtomicInteger(), cancelled, Duration.ofSeconds(2)));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("evt-3", "test.event")))
                .expectError(TransientEventException.class)
                .verify(Duration.ofSeconds(3));

        assertEquals(0, store.completions.get());
        assertEquals(1, cancelled.get(), "el handler debe cancelarse al perder el claim");
    }

    // ---- helpers ----

    private static Mono<Object> outcome(Mono<Void> dispatch) {
        return dispatch
                .thenReturn((Object) "OK")
                .onErrorResume(error -> Mono.just((Object) error))
                .subscribeOn(Schedulers.parallel());
    }

    private static EventDispatcher dispatcher(
            IdempotencyStore store, Duration ttl, IntegrationEventHandler<TestEvent> handler) {
        List<IntegrationEventHandler<?>> handlers = List.<IntegrationEventHandler<?>>of(handler);
        return new EventDispatcher(handlers, store, "test-instance", ttl);
    }

    private static IntegrationEventHandler<TestEvent> handler(
            AtomicInteger runs, AtomicInteger cancelled, Duration work) {
        return new IntegrationEventHandler<>() {
            @Override
            public Mono<Void> handle(TestEvent event) {
                return Mono.defer(() -> {
                    runs.incrementAndGet();
                    return Mono.delay(work).then().doOnCancel(cancelled::incrementAndGet);
                });
            }

            @Override
            public String getEventName() {
                return "test.event";
            }
        };
    }

    /** Store en memoria con la misma semántica (y expiración) que el de Redis. */
    private static final class ExpiringStore implements IdempotencyStore {

        private record Entry(String state, long expiresAtNanos) {}

        private final Map<String, Entry> entries = new ConcurrentHashMap<>();
        final AtomicInteger renewals = new AtomicInteger();
        final AtomicInteger completions = new AtomicInteger();
        volatile boolean rejectRenewals;

        @Override
        public Mono<ClaimResult> tryClaim(String eventId, String owner, Duration ttl) {
            return Mono.fromSupplier(() -> {
                long now = System.nanoTime();
                ClaimResult[] result = new ClaimResult[1];
                entries.compute(eventId, (key, current) -> {
                    if (current == null || current.expiresAtNanos() <= now) {
                        result[0] = ClaimResult.CLAIMED;
                        return new Entry("IN_PROGRESS:" + owner, now + ttl.toNanos());
                    }
                    result[0] = "COMPLETED".equals(current.state())
                            ? ClaimResult.COMPLETED : ClaimResult.IN_PROGRESS;
                    return current;
                });
                return result[0];
            });
        }

        @Override
        public Mono<Boolean> renewClaim(String eventId, String owner, Duration ttl) {
            return Mono.fromSupplier(() -> {
                if (rejectRenewals) {
                    return false;
                }
                long now = System.nanoTime();
                boolean[] renewed = new boolean[1];
                entries.computeIfPresent(eventId, (key, current) -> {
                    if (current.state().equals("IN_PROGRESS:" + owner)
                            && current.expiresAtNanos() > now) {
                        renewed[0] = true;
                        renewals.incrementAndGet();
                        return new Entry(current.state(), now + ttl.toNanos());
                    }
                    return current;
                });
                return renewed[0];
            });
        }

        @Override
        public Mono<Boolean> markCompleted(String eventId, String owner) {
            return Mono.fromSupplier(() -> {
                long now = System.nanoTime();
                boolean[] done = new boolean[1];
                entries.computeIfPresent(eventId, (key, current) -> {
                    if (current.state().equals("IN_PROGRESS:" + owner)
                            && current.expiresAtNanos() > now) {
                        done[0] = true;
                        completions.incrementAndGet();
                        return new Entry("COMPLETED", now + Duration.ofHours(1).toNanos());
                    }
                    return current;
                });
                return done[0];
            });
        }

        @Override
        public Mono<Boolean> releaseClaim(String eventId, String owner) {
            return Mono.fromSupplier(() -> {
                boolean[] released = new boolean[1];
                entries.computeIfPresent(eventId, (key, current) -> {
                    if (current.state().equals("IN_PROGRESS:" + owner)) {
                        released[0] = true;
                        return null;
                    }
                    return current;
                });
                return released[0];
            });
        }
    }
}
