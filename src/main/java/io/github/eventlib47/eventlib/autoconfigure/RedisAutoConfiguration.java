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
package io.github.eventlib47.eventlib.autoconfigure;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore;
import io.github.eventlib47.eventlib.consumer.idempotency.ReactiveRedisIdempotencyStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Auto-configuración de idempotencia.
 *
 * <p>La fuente de verdad es Spring Boot (`spring.data.redis.*`). El
 * fallback `eventlib.infra.redis.*` existe solo para compatibilidad. Si no
 * hay Redis y el consumidor está activo, el contexto falla cerrado.</p>
 */
@Slf4j
@AutoConfiguration
@AutoConfigureAfter(org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class)
public class RedisAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(RedisConnectionFactory.class)
    @ConditionalOnProperty(prefix = "eventlib.infra.redis", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public LettuceConnectionFactory redisConnectionFactory(InfraProperties infraProps) {
        var redisConfig = infraProps.getRedis();
        var config = new RedisStandaloneConfiguration(
                redisConfig.getHost(), redisConfig.getPort());

        if (redisConfig.getPassword() != null && !redisConfig.getPassword().isBlank()) {
            config.setPassword(redisConfig.getPassword());
        }

        LettuceClientConfiguration.LettuceClientConfigurationBuilder clientBuilder =
                LettuceClientConfiguration.builder();
        if (redisConfig.isSslEnabled()) {
            clientBuilder.useSsl();
        }

        log.warn("[eventlib] spring.data.redis.* no estaba configurado; usando fallback "
                        + "eventlib.infra.redis.* en {}:{} (ssl={}).",
                redisConfig.getHost(), redisConfig.getPort(), redisConfig.isSslEnabled());

        return new LettuceConnectionFactory(config, clientBuilder.build());
    }

    @Slf4j
    @Configuration
    @ConditionalOnClass({ReactiveStringRedisTemplate.class, ReactiveRedisConnectionFactory.class})
    @ConditionalOnProperty(prefix = "eventlib.infra.redis", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    static class ReactiveConfig {

        @Bean
        @ConditionalOnMissingBean(ReactiveStringRedisTemplate.class)
        public ReactiveStringRedisTemplate reactiveStringRedisTemplate(
                ReactiveRedisConnectionFactory factory) {
            return new ReactiveStringRedisTemplate(factory);
        }

        @Bean
        @ConditionalOnMissingBean(IdempotencyStore.class)
        public IdempotencyStore reactiveIdempotencyStore(
                ReactiveStringRedisTemplate reactiveTemplate,
                InfraProperties props) {
            log.info("[eventlib] Idempotencia atómica configurada sobre Redis.");
            return new ReactiveRedisIdempotencyStore(reactiveTemplate, props);
        }
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyStore.class)
    @ConditionalOnProperty(prefix = "eventlib.infra.redis", name = "allow-no-op",
            havingValue = "true")
    public IdempotencyStore noOpIdempotencyStore() {
        log.warn("[eventlib] Idempotencia NULA habilitada explícitamente. "
                + "No usar en producción.");
        return new IdempotencyStore() {
            @Override
            public Mono<ClaimResult> tryClaim(String eventId, String owner, Duration ttl) {
                return Mono.just(ClaimResult.CLAIMED);
            }

            @Override
            public Mono<Boolean> markCompleted(String eventId, String owner) {
                return Mono.just(true);
            }

            @Override
            public Mono<Boolean> releaseClaim(String eventId, String owner) {
                return Mono.just(true);
            }
        };
    }
}
