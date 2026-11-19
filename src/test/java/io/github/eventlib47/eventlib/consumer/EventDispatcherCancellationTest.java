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
import io.github.eventlib47.eventlib.support.RecordingIdempotencyStore;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cubre el ciclo de vida del claim ante cancelación y ante fallos sostenidos
 * del store durante el heartbeat.
 */
class EventDispatcherCancellationTest {

    private static final String EVENT_NAME = "test.event";

    /** Construye un dispatcher con un único handler que ejecuta {@code work}. */
    private static EventDispatcher dispatcher(
            IdempotencyStore store, Mono<Void> work, Duration claimTtl) {
        IntegrationEventHandler<TestEvent> handler = new IntegrationEventHandler<>() {
            @Override
            public Mono<Void> handle(TestEvent event) {
                return work;
            }

            @Override
            public String getEventName() {
                return EVENT_NAME;
            }
        };
        return new EventDispatcher(
                List.<IntegrationEventHandler<?>>of(handler),
                store,
                "test-instance",
                claimTtl);
    }

    @Test
    void cancellationWhileHandlerRuns_releasesTheClaimAndNeverCompletes() {
        RecordingIdempotencyStore store = new RecordingIdempotencyStore();
        // El handler nunca termina: simula un proceso largo que será cancelado.
        EventDispatcher dispatcher = dispatcher(store, Mono.never(), Duration.ofSeconds(30));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("evt-cancel", EVENT_NAME)))
                .expectSubscription()
                .thenCancel()
                .verify(Duration.ofSeconds(2));

        assertThat(store.claims.get()).isEqualTo(1);
        // La liberación por cancelación debe haberse intentado exactamente una vez.
        assertThat(store.releases.get()).isEqualTo(1);
        assertThat(store.completions.get()).isZero();
    }

    @Test
    void sustainedRenewalFailure_abortsTheHandlerAsTransient() {
        // Redis "caído" para renovar: el claim real expirará pasado el TTL.
        RecordingIdempotencyStore store = new RecordingIdempotencyStore() {
            @Override
            public Mono<Boolean> renewClaim(String eventId, String owner, Duration ttl) {
                return Mono.error(new IllegalStateException("redis down"));
            }
        };
        // TTL 300 ms → latido cada 100 ms → a los ~300 ms se considera perdido.
        EventDispatcher dispatcher =
                dispatcher(store, Mono.never(), Duration.ofMillis(300));

        StepVerifier.create(dispatcher.dispatch(new TestEvent("evt-renew", EVENT_NAME)))
                .expectErrorSatisfies(error -> assertThat(error)
                        .isInstanceOf(TransientEventException.class)
                        .hasMessageContaining("claim"))
                .verify(Duration.ofSeconds(3));

        assertThat(store.completions.get()).isZero();
        // El camino de error libera el claim (fenced) antes de propagar.
        assertThat(store.releases.get()).isEqualTo(1);
    }
}