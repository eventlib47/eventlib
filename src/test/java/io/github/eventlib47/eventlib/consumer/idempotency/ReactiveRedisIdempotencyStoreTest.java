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
package io.github.eventlib47.eventlib.consumer.idempotency;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore.ClaimResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ReactiveRedisIdempotencyStoreTest {

    private ReactiveStringRedisTemplate redis;
    private InfraProperties infra;
    private ReactiveRedisIdempotencyStore store;

    @BeforeEach
    void setUp() {
        redis = mock(ReactiveStringRedisTemplate.class);
        infra = new InfraProperties();
        infra.getRedis().setIdempotencyPrefix("test:idem");
        infra.getRedis().setClaimTtl(Duration.ofSeconds(30));
        infra.getRedis().setIdempotencyTtl(Duration.ofHours(1));
        store = new ReactiveRedisIdempotencyStore(redis, infra);
    }

    @Test
    void validatesConstructorArgumentsAndProperties() {
        assertThrows(NullPointerException.class, () -> new ReactiveRedisIdempotencyStore(null, infra));
        assertThrows(NullPointerException.class, () -> new ReactiveRedisIdempotencyStore(redis, null));

        InfraProperties badInfra = new InfraProperties();
        badInfra.getRedis().setIdempotencyPrefix(" ");
        assertThrows(IllegalArgumentException.class, () -> new ReactiveRedisIdempotencyStore(redis, badInfra));

        InfraProperties badTtl = new InfraProperties();
        badTtl.getRedis().setClaimTtl(Duration.ZERO);
        assertThrows(IllegalArgumentException.class, () -> new ReactiveRedisIdempotencyStore(redis, badTtl));
    }

    @Test
    @SuppressWarnings("unchecked")
    void tryClaimMapsLuaScriptResponses() {
        when(redis.execute(any(RedisScript.class), eq(List.of("test:idem:evt-1")), eq(List.of("owner-1", "30000"))))
                .thenReturn(Flux.just(1L))
                .thenReturn(Flux.just(2L))
                .thenReturn(Flux.just(3L))
                .thenReturn(Flux.just(99L))
                .thenReturn(Flux.empty());

        StepVerifier.create(store.tryClaim("evt-1", "owner-1", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED)
                .verifyComplete();

        StepVerifier.create(store.tryClaim("evt-1", "owner-1", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.IN_PROGRESS)
                .verifyComplete();

        StepVerifier.create(store.tryClaim("evt-1", "owner-1", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.COMPLETED)
                .verifyComplete();

        StepVerifier.create(store.tryClaim("evt-1", "owner-1", Duration.ofSeconds(30)))
                .expectError(IllegalStateException.class)
                .verify();

        StepVerifier.create(store.tryClaim("evt-1", "owner-1", Duration.ofSeconds(30)))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    void tryClaimRejectsInvalidInputs() {
        assertThrows(IllegalArgumentException.class,
                () -> store.tryClaim("", "owner-1", Duration.ofSeconds(10)));
        assertThrows(IllegalArgumentException.class,
                () -> store.tryClaim("x".repeat(257), "owner-1", Duration.ofSeconds(10)));
        assertThrows(IllegalArgumentException.class,
                () -> store.tryClaim("evt-1", " ", Duration.ofSeconds(10)));
        assertThrows(IllegalArgumentException.class,
                () -> store.tryClaim("evt-1", "owner-1", Duration.ofMillis(-5)));
    }

    @Test
    @SuppressWarnings("unchecked")
    void renewCompleteAndReleaseHandleOwnership() {
        when(redis.execute(any(RedisScript.class), eq(List.of("test:idem:evt-1")), eq(List.of("owner-1", "15000"))))
                .thenReturn(Flux.just(1L))
                .thenReturn(Flux.just(0L));

        StepVerifier.create(store.renewClaim("evt-1", "owner-1", Duration.ofSeconds(15)))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(store.renewClaim("evt-1", "owner-1", Duration.ofSeconds(15)))
                .expectNext(false)
                .verifyComplete();

        when(redis.execute(any(RedisScript.class), eq(List.of("test:idem:evt-1")), eq(List.of("owner-1", "3600000"))))
                .thenReturn(Flux.just(1L))
                .thenReturn(Flux.empty());

        StepVerifier.create(store.markCompleted("evt-1", "owner-1"))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(store.markCompleted("evt-1", "owner-1"))
                .expectNext(false)
                .verifyComplete();

        when(redis.execute(any(RedisScript.class), eq(List.of("test:idem:evt-1")), eq(List.of("owner-1"))))
                .thenReturn(Flux.just(1L))
                .thenReturn(Flux.just(0L));

        StepVerifier.create(store.releaseClaim("evt-1", "owner-1"))
                .expectNext(true)
                .verifyComplete();
        StepVerifier.create(store.releaseClaim("evt-1", "owner-1"))
                .expectNext(false)
                .verifyComplete();
    }
}
