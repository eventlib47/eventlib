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
package io.github.eventlib47.eventlib.health;

import io.github.eventlib47.eventlib.config.InfraProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Expone configuración y conectividad de EventLib en `/actuator/health`.
 *
 * <p>Si hay Redis configurado hace un `PING` con timeout corto. RabbitMQ
 * no se consulta aquí para no acoplar el health check al broker; el
 * publisher ya falla cerrado por confirmación.</p>
 */
public class EventLibHealthIndicator implements ReactiveHealthIndicator {

    private static final Duration REDIS_TIMEOUT = Duration.ofSeconds(2);

    private final InfraProperties infraProperties;
    private final ReactiveStringRedisTemplate redis;

    public EventLibHealthIndicator(
            InfraProperties infraProperties,
            ReactiveStringRedisTemplate redis) {
        this.infraProperties = infraProperties;
        this.redis = redis;
    }

    @Override
    public Mono<Health> health() {
        Health.Builder builder = Health.up()
                .withDetail("rabbitExchange",
                        infraProperties.getRabbitmq().getExchange())
                .withDetail("rabbitQueueType",
                        infraProperties.getRabbitmq().getQueueType())
                .withDetail("outboxEnabled",
                        infraProperties.getOutbox().isEnabled())
                .withDetail("redisNoOpAllowed",
                        infraProperties.getRedis().isAllowNoOp());

        if (redis == null || redis.getConnectionFactory() == null) {
            return Mono.just(builder.withDetail("redis", "not-configured").build());
        }

        return redis.getConnectionFactory()
                .getReactiveConnection()
                .ping()
                .timeout(REDIS_TIMEOUT)
                .map(pong -> builder.withDetail("redis", "UP")
                        .withDetail("redisPing", pong)
                        .build())
                .onErrorResume(error -> Mono.just(Health.down()
                        .withException(error)
                        .withDetail("redis", "DOWN")
                        .build()));
    }
}
