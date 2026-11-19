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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.health.EventLibHealthIndicator;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.actuate.health.ReactiveHealthIndicator;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * Auto-configuración base de EventLib: propiedades, serializador, métricas
 * y (si Actuator está presente) el health indicator.
 */
@AutoConfiguration
@EnableConfigurationProperties(InfraProperties.class)
public class EventLibAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public MessageSerializer messageSerializer(ObjectMapper objectMapper) {
        return new MessageSerializer(objectMapper);
    }

    /** Usa el MeterRegistry de la aplicación si existe; si no, métricas no-op. */
    @Bean
    @ConditionalOnMissingBean
    public EventLibMetrics eventLibMetrics(ObjectProvider<MeterRegistry> registryProvider) {
        return new EventLibMetrics(registryProvider.getIfAvailable());
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ReactiveHealthIndicator.class)
    static class ActuatorConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "eventLibHealthIndicator")
        public ReactiveHealthIndicator eventLibHealthIndicator(
                InfraProperties infraProperties,
                ObjectProvider<ReactiveStringRedisTemplate> redisProvider) {
            return new EventLibHealthIndicator(
                    infraProperties, redisProvider.getIfAvailable());
        }
    }
}
