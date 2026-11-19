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
package io.github.eventlib47.eventlib.broker;

import io.github.eventlib47.eventlib.core.IntegrationEvent;
import reactor.core.publisher.Flux;

/**
 * Puerto de consumo, agnóstico al broker — simétrico a {@link MessagePublisher}
 * del lado productor.
 *
 * {@code RabbitMessageReceiver} es la única implementación hoy. El día que
 * exista un {@code KafkaMessageReceiver}, el microservicio consumidor
 * cambia de broker reemplazando una propiedad de configuración, no
 * código de aplicación — porque {@code EventSubscriptionConfig} pasa a
 * inyectar {@link MessageReceiver} (el puerto), no la clase concreta.
 *
 * Antes de esta interfaz, {@code RabbitMessageReceiver} se inyectaba
 * directamente por tipo concreto en el código de cada microservicio —
 * era el bloqueador real para soportar múltiples brokers del lado consumidor.
 */
public interface MessageReceiver {

    /**
     * @param destination nombre de la cola (RabbitMQ) o del tópico (Kafka)
     * @param eventType   tipo concreto al que se deserializa cada mensaje
     */
    <T extends IntegrationEvent> Flux<T> consume(String destination, Class<T> eventType);
}
