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
package io.github.eventlib47.eventlib.producer.routing;

import io.github.eventlib47.eventlib.config.RabbitProperties;

import java.util.Objects;

public record EventRoute(
        String alias,
        String eventName,
        String exchange,
        String routingKey,
        String queueName,
        RabbitProperties.ExchangeType exchangeType,
        int shardCount,
        boolean declareInfrastructure,
        String virtualHost
) {

    public EventRoute {
        alias = requireText(alias, "alias");
        eventName = requireText(eventName, "eventName");
        exchange = requireText(exchange, "exchange");
        routingKey = requireText(routingKey, "routingKey");

        if (queueName != null && queueName.isBlank()) {
            queueName = null;
        }

        exchangeType = Objects.requireNonNull(
                exchangeType, "exchangeType no puede ser null");

        if (exchangeType == RabbitProperties.ExchangeType.X_CONSISTENT_HASH
                && shardCount < 1) {
            throw new IllegalArgumentException(
                    "shardCount debe ser mayor que cero para X_CONSISTENT_HASH.");
        }

        if (virtualHost != null && virtualHost.isBlank()) {
            virtualHost = null;
        }
    }

    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    fieldName + " no puede ser null ni estar vacío.");
        }
        return value.trim();
    }

    public boolean hasQueue() { return queueName != null; }

    public boolean requiresInfrastructureDeclaration() {
        return declareInfrastructure;
    }
}