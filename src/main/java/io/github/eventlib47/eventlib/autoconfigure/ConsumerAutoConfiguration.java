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

import io.github.eventlib47.eventlib.broker.rabbit.RabbitMessageReceiver;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.IntegrationEventHandler;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import reactor.rabbitmq.Receiver;

import java.util.List;
import java.util.UUID;

/**
 * Auto-configuración del lado consumidor de eventlib.
 *
 * <p>Si hay handlers registrados, la idempotencia es obligatoria. Sin
 * Redis el contexto falla cerrado, salvo que se habilite explícitamente
 * el store nulo para pruebas.</p>
 */
@Slf4j
@AutoConfiguration(after = {RabbitAutoConfiguration.class, RedisAutoConfiguration.class})
@ConditionalOnBean(IntegrationEventHandler.class)
public class ConsumerAutoConfiguration {

    @Bean
    public EventDispatcher eventDispatcher(
            List<IntegrationEventHandler<?>> handlers,
            IdempotencyStore store,
            InfraProperties infraProperties,
            @Value("${spring.application.name:unknown-service}") String appName) {

        String owner = appName + ":" + UUID.randomUUID();
        log.info("[eventlib] ConsumerAutoConfiguration activa — {} handler(s), owner={}",
                handlers.size(), owner);
        return new EventDispatcher(
                handlers,
                store,
                owner,
                infraProperties.getRedis().getClaimTtl());
    }

    @Bean
    public RabbitMessageReceiver rabbitMessageReceiver(
            Receiver receiver,
            MessageSerializer serializer,
            EventDispatcher dispatcher,
            InfraProperties infraProperties) {
        return new RabbitMessageReceiver(
                receiver,
                serializer,
                dispatcher,
                infraProperties.getRabbitmq().getConsumerRetryDelay(),
                infraProperties.getRabbitmq().getConsumerPrefetch());
    }
}
