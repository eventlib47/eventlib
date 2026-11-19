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

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Calcula la clave de hash (routing key) que RabbitMQ usa para elegir el
 * shard de destino cuando un evento se publica en un exchange
 * x-consistent-hash.
 *
 * Opera sobre el JSON ya serializado del evento — no sobre el objeto
 * {@code IntegrationEvent} tipado — porque el Outbox reconstruye el
 * payload desde la base de datos como JSON crudo; un evento tipado no
 * siempre está disponible en el momento de publicar.
 *
 * Registra tu propia implementación como {@code @Bean} para sobreescribir
 * {@link DefaultHashKeyResolver} sin tocar el código de la librería.
 */
public interface HashKeyResolver {

    /**
     * @param eventName nombre lógico del evento (eventName / routingKey en modo TOPIC)
     * @param eventJson JSON completo del evento, incluye el nodo "metadata"
     * @return la clave que RabbitMQ hasheará para elegir el shard
     */
    String resolve(String eventName, JsonNode eventJson);
}
