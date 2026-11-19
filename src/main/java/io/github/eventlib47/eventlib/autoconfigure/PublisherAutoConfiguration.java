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

import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.DirectEventPublisher;
import io.github.eventlib47.eventlib.producer.EventPublisher;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

/**
 * Registra {@link DirectEventPublisher} cuando el Outbox está DESACTIVADO
 * (valor por defecto). Con el Outbox activo, el {@code EventPublisher} lo
 * aporta {@link OutboxAutoConfiguration}.
 */
@AutoConfiguration(after = {
        EventLibAutoConfiguration.class,
        RabbitAutoConfiguration.class,
        OutboxAutoConfiguration.class})
@ConditionalOnProperty(prefix = "eventlib.infra.outbox", name = "enabled",
        havingValue = "false", matchIfMissing = true)
@ConditionalOnBean({MessagePublisher.class, MessageSerializer.class, EventRoutingResolver.class})
public class PublisherAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(EventPublisher.class)
    public DirectEventPublisher eventPublisher(
            MessagePublisher messagePublisher,
            MessageSerializer serializer,
            EventRoutingResolver routingResolver) {
        return new DirectEventPublisher(messagePublisher, serializer, routingResolver);
    }
}
