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
package io.github.eventlib47.eventlib.producer;

import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import reactor.core.publisher.Mono;

public class DirectEventPublisher implements EventPublisher {

    private final MessagePublisher messagePublisher;
    private final MessageSerializer serializer;
    private final EventRoutingResolver routingResolver;

    public DirectEventPublisher(
            MessagePublisher messagePublisher,
            MessageSerializer serializer,
            EventRoutingResolver routingResolver) {
        this.messagePublisher = messagePublisher;
        this.serializer = serializer;
        this.routingResolver = routingResolver;
    }

    @Override
    public Mono<Void> publish(IntegrationEvent event) {
        if (event == null) {
            return Mono.error(new IllegalArgumentException(
                    "El evento que se va a publicar no puede ser null."));
        }

        return Mono.defer(() -> {
            EventRoute route = routingResolver.resolve(event);

            if (route == null) {
                return Mono.error(new IllegalStateException(
                        "EventRoutingResolver no resolvió una ruta para el evento: "
                                + event.getEventName()));
            }

            String routingKey =
                    route.exchangeType() == RabbitProperties.ExchangeType.X_CONSISTENT_HASH
                            ? event.getAggregateId()
                            : route.routingKey();

            if (routingKey == null || routingKey.isBlank()) {
                return Mono.error(new IllegalStateException(
                        "La routing key está vacía para el evento: "
                                + event.getEventName()));
            }

            byte[] payload = serializer.serializev(event);

            return messagePublisher.publish(
                    route.exchange(),
                    routingKey,
                    payload);
        });
    }
}