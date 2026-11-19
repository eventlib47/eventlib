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
package io.github.eventlib47.eventlib.broker.rabbit.routing;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;

public class DefaultEventRoutingResolver implements EventRoutingResolver {

    private final RabbitProperties rabbit;

    public DefaultEventRoutingResolver(InfraProperties infraProperties) {
        Objects.requireNonNull(infraProperties, "infraProperties no puede ser null");
        this.rabbit = Objects.requireNonNull(
                infraProperties.getRabbitmq(),
                "La configuración eventlib.infra.rabbitmq es obligatoria"
        );
    }

    @Override
    public EventRoute resolve(IntegrationEvent event) {
        Objects.requireNonNull(event, "event no puede ser null");
        return resolve(requireEventName(event.getEventName()));
    }

    @Override
    public EventRoute resolve(String eventName) {
        String name = requireEventName(eventName);

        String exchange = valueOrDefault(
                getValue(rabbit.getExchanges(), name),
                rabbit.getExchange(),
                "eventlib.exchange"
        );

        String routingKey = valueOrDefault(
                getValue(rabbit.getBindings(), name),
                name,
                name
        );

        String queueName = getValue(rabbit.getQueues(), name);

        RabbitProperties.ExchangeType exchangeType = getValue(
                rabbit.getExchangeTypes(), name
        );
        if (exchangeType == null) {
            exchangeType = RabbitProperties.ExchangeType.TOPIC;
        }

        Integer configuredShards = getValue(rabbit.getShards(), name);
        int shardCount = configuredShards != null ? configuredShards : 4;

        Boolean declareOverride = getValue(
                rabbit.getDeclareInfrastructureOverrides(), name
        );
        boolean declareInfrastructure = declareOverride != null
                ? declareOverride
                : rabbit.isDeclareInfrastructure();

        String virtualHost = valueOrDefault(
                getValue(rabbit.getExchangeVirtualHosts(), name),
                rabbit.getVirtualHost(),
                null
        );

        return new EventRoute(
                name,
                name,
                exchange,
                routingKey,
                queueName,
                exchangeType,
                shardCount,
                declareInfrastructure,
                virtualHost
        );
    }

    @Override
    public Collection<EventRoute> resolveAll() {
        TreeSet<String> eventNames = new TreeSet<>();
        addKeys(eventNames, rabbit.getQueues());
        addKeys(eventNames, rabbit.getBindings());
        addKeys(eventNames, rabbit.getExchanges());
        addKeys(eventNames, rabbit.getExchangeTypes());
        addKeys(eventNames, rabbit.getShards());
        addKeys(eventNames, rabbit.getDeclareInfrastructureOverrides());
        addKeys(eventNames, rabbit.getExchangeVirtualHosts());

        if (eventNames.isEmpty()) {
            return Collections.emptyList();
        }

        Collection<EventRoute> routes = new ArrayList<>(eventNames.size());
        for (String eventName : eventNames) {
            routes.add(resolve(eventName));
        }
        return Collections.unmodifiableCollection(routes);
    }

    @Override
    public String defaultExchange() {
        return valueOrDefault(
                rabbit.getExchange(),
                "eventlib.exchange",
                "eventlib.exchange"
        );
    }

    private static <T> T getValue(Map<String, T> values, String key) {
        return values == null ? null : values.get(key);
    }

    private static void addKeys(TreeSet<String> target, Map<String, ?> source) {
        if (source != null) {
            source.keySet().stream()
                    .filter(Objects::nonNull)
                    .filter(key -> !key.isBlank())
                    .forEach(target::add);
        }
    }

    private static String requireEventName(String eventName) {
        if (eventName == null || eventName.isBlank()) {
            throw new IllegalArgumentException(
                    "El nombre del evento no puede ser null ni estar vacío"
            );
        }
        return eventName.trim();
    }

    private static String valueOrDefault(
            String value, String fallback, String finalDefault) {
        if (value != null && !value.isBlank()) return value.trim();
        if (fallback != null && !fallback.isBlank()) return fallback.trim();
        return finalDefault;
    }
}