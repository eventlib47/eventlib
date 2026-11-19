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
import io.github.eventlib47.eventlib.config.RedisProperties;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Implementación Redis con scripts Lua atómicos y fencing por owner.
 *
 * <p>Valores almacenados: {@code COMPLETED} o {@code IN_PROGRESS:<owner>}.</p>
 *
 * <ul>
 *   <li>{@code tryClaim}: SET NX PX atómico. Cualquier otra entrega (incluso de
 *       la misma instancia, porque su owner es distinto) recibe IN_PROGRESS.</li>
 *   <li>{@code renewClaim}: PEXPIRE solo si el owner coincide (heartbeat).</li>
 *   <li>{@code markCompleted} / {@code releaseClaim}: condicionados al owner.</li>
 * </ul>
 */
public class ReactiveRedisIdempotencyStore implements IdempotencyStore {

    /** 1 = reclamado, 2 = en progreso por otro, 3 = ya completado. */
    private static final DefaultRedisScript<Long> CLAIM_SCRIPT = new DefaultRedisScript<>(
            "local current = redis.call('GET', KEYS[1]) " +
                    "if not current then " +
                    "  redis.call('SET', KEYS[1], 'IN_PROGRESS:' .. ARGV[1], 'PX', ARGV[2]) " +
                    "  return 1 " +
                    "end " +
                    "if current == 'COMPLETED' then return 3 end " +
                    "return 2",
            Long.class);

    /** 1 = renovado; 0 = el claim expiró o es de otro owner. */
    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == 'IN_PROGRESS:' .. ARGV[1] then " +
                    "  return redis.call('PEXPIRE', KEYS[1], ARGV[2]) " +
                    "end " +
                    "return 0",
            Long.class);

    private static final DefaultRedisScript<Long> COMPLETE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == 'IN_PROGRESS:' .. ARGV[1] then " +
                    "  redis.call('SET', KEYS[1], 'COMPLETED', 'PX', ARGV[2]) " +
                    "  return 1 " +
                    "end " +
                    "return 0",
            Long.class);

    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('GET', KEYS[1]) == 'IN_PROGRESS:' .. ARGV[1] then " +
                    "  return redis.call('DEL', KEYS[1]) " +
                    "end " +
                    "return 0",
            Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final RedisProperties properties;

    public ReactiveRedisIdempotencyStore(
            ReactiveStringRedisTemplate redis,
            InfraProperties infra) {
        this.redis = Objects.requireNonNull(redis, "redis no puede ser null");
        Objects.requireNonNull(infra, "infra no puede ser null");
        this.properties = Objects.requireNonNull(
                infra.getRedis(), "eventlib.infra.redis es obligatorio");
        // Fail-fast: configuración inválida rompe el arranque, no el runtime.
        validateProperties(this.properties);
    }

    @Override
    public Mono<ClaimResult> tryClaim(String eventId, String owner, Duration ttl) {
        String key = key(eventId);
        String safeOwner = requireText(owner, "owner");
        Duration effectiveTtl = requirePositive(ttl, "ttl");

        return redis.execute(
                        CLAIM_SCRIPT,
                        List.of(key),
                        List.of(safeOwner, Long.toString(effectiveTtl.toMillis())))
                .next()
                .map(value -> {
                    if (value == 1L) return ClaimResult.CLAIMED;
                    if (value == 2L) return ClaimResult.IN_PROGRESS;
                    if (value == 3L) return ClaimResult.COMPLETED;
                    throw new IllegalStateException(
                            "Resultado inesperado del script de idempotencia: " + value);
                })
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Redis no devolvió resultado para el claim de " + key)));
    }

    @Override
    public Mono<Boolean> renewClaim(String eventId, String owner, Duration ttl) {
        String key = key(eventId);
        String safeOwner = requireText(owner, "owner");
        Duration effectiveTtl = requirePositive(ttl, "ttl");

        return redis.execute(
                        RENEW_SCRIPT,
                        List.of(key),
                        List.of(safeOwner, Long.toString(effectiveTtl.toMillis())))
                .next()
                .map(value -> value == 1L)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Boolean> markCompleted(String eventId, String owner) {
        String key = key(eventId);
        String safeOwner = requireText(owner, "owner");
        Duration ttl = properties.getIdempotencyTtl();

        return redis.execute(
                        COMPLETE_SCRIPT,
                        List.of(key),
                        List.of(safeOwner, Long.toString(ttl.toMillis())))
                .next()
                .map(value -> value == 1L)
                .defaultIfEmpty(false);
    }

    @Override
    public Mono<Boolean> releaseClaim(String eventId, String owner) {
        String key = key(eventId);
        String safeOwner = requireText(owner, "owner");

        return redis.execute(
                        RELEASE_SCRIPT,
                        List.of(key),
                        List.of(safeOwner))
                .next()
                .map(value -> value == 1L)
                .defaultIfEmpty(false);
    }

    private void validateProperties(RedisProperties props) {
        requireText(props.getIdempotencyPrefix(), "idempotencyPrefix");
        requirePositive(props.getIdempotencyTtl(), "idempotencyTtl");
        requirePositive(props.getClaimTtl(), "claimTtl");
    }

    private String key(String eventId) {
        String safeEventId = requireText(eventId, "eventId");
        if (safeEventId.length() > 256) {
            throw new IllegalArgumentException(
                    "eventId excede el máximo de 256 caracteres.");
        }
        String prefix = requireText(properties.getIdempotencyPrefix(), "idempotencyPrefix");
        return prefix + ":" + safeEventId;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " no puede ser null ni estar vacío.");
        }
        return value.trim();
    }

    private static Duration requirePositive(Duration value, String field) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " debe ser mayor que cero.");
        }
        return value;
    }
}
