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

import io.github.eventlib47.eventlib.config.InfraProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * Lease distribuido en Redis. El owner se renueva mientras procesa el
 * batch y solo el owner puede liberar el lock.
 */
@Slf4j
public class OutboxDistributedLock {

    private static final String LOCK_KEY_PREFIX = "eventlib:outbox:leader-lock:";

    private static final DefaultRedisScript<Long> RELEASE_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  return redis.call('del', KEYS[1]) "
                    + "else "
                    + "  return 0 "
                    + "end", Long.class);

    private static final DefaultRedisScript<Long> RENEW_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then "
                    + "  return redis.call('pexpire', KEYS[1], ARGV[2]) "
                    + "else "
                    + "  return 0 "
                    + "end", Long.class);

    private final ReactiveStringRedisTemplate redis;
    private final String lockKey;
    private final String instanceId;
    private final Duration ttl;

    public OutboxDistributedLock(
            ReactiveStringRedisTemplate redis,
            InfraProperties infra,
            String appName) {
        this.redis = redis;
        this.lockKey = LOCK_KEY_PREFIX + appName;
        this.instanceId = appName + ":" + UUID.randomUUID();
        Duration configured = infra.getOutbox().getLockTtl();
        this.ttl = configured == null || configured.isZero() || configured.isNegative()
                ? Duration.ofSeconds(60)
                : configured;
    }

    public Mono<Boolean> tryAcquire() {
        return redis.opsForValue()
                .setIfAbsent(lockKey, instanceId, ttl)
                .defaultIfEmpty(false)
                .doOnNext(acquired -> {
                    if (Boolean.TRUE.equals(acquired)) {
                        log.debug("[eventlib] Lease del Outbox adquirido por {}",
                                instanceId);
                    }
                });
    }

    public Mono<Boolean> renew() {
        return redis.execute(
                        RENEW_SCRIPT,
                        List.of(lockKey),
                        List.of(instanceId, Long.toString(ttl.toMillis())))
                .next()
                .map(value -> value == 1L)
                .defaultIfEmpty(false);
    }

    public Mono<Void> release() {
        return redis.execute(
                        RELEASE_SCRIPT,
                        List.of(lockKey),
                        List.of(instanceId))
                .next()
                .doOnNext(deleted -> {
                    if (deleted != null && deleted == 1L) {
                        log.debug("[eventlib] Lease del Outbox liberado por {}",
                                instanceId);
                    }
                })
                .timeout(Duration.ofSeconds(5))
                .onErrorResume(error -> {
                    log.warn("No se pudo liberar el lease del Outbox: {}",
                            error.getMessage());
                    return Mono.empty();
                })
                .then();
    }
}
