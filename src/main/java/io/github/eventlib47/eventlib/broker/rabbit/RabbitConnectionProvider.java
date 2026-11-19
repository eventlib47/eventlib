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
package io.github.eventlib47.eventlib.broker.rabbit;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Proveedor de conexiones AMQP (productor y consumidor separadas).
 *
 * <p>Si una conexión cae sin que la aplicación la cierre, se reemplaza el
 * {@code Mono} cacheado: los siguientes suscriptores reciben una conexión
 * nueva. {@link #destroy()} cierra todo y detiene la reconexión.</p>
 */
@Slf4j
public class RabbitConnectionProvider implements DisposableBean {

    private final ConnectionFactory factory;
    private final String appName;
    private final ResilienceProperties properties;
    private final EventLibMetrics metrics;

    private final AtomicReference<Mono<Connection>> producerRef = new AtomicReference<>();
    private final AtomicReference<Mono<Connection>> consumerRef = new AtomicReference<>();
    private final List<Connection> openConnections = new CopyOnWriteArrayList<>();
    private final AtomicBoolean closing = new AtomicBoolean(false);

    public RabbitConnectionProvider(
            ConnectionFactory factory,
            String appName,
            ResilienceProperties properties,
            EventLibMetrics metrics) {
        this.factory = factory;
        this.appName = appName;
        this.properties = properties == null ? new ResilienceProperties() : properties;
        this.metrics = metrics == null ? EventLibMetrics.noop() : metrics;
        producerRef.set(build("PRODUCER", producerRef));
        consumerRef.set(build("CONSUMER", consumerRef));
    }

    public Mono<Connection> producerConnection() {
        return Mono.defer(producerRef::get);
    }

    public Mono<Connection> consumerConnection() {
        return Mono.defer(consumerRef::get);
    }

    private Mono<Connection> build(String type, AtomicReference<Mono<Connection>> ref) {
        String name = appName + "-" + type.toLowerCase(Locale.ROOT);
        CircuitBreaker circuitBreaker =
                ResiliencePolicy.newCircuitBreaker(name + "-connection", properties);
        Bulkhead bulkhead = ResiliencePolicy.newBulkhead(name + "-connection", properties);

        return Mono.fromCallable(() -> {
                    if (closing.get()) {
                        throw new IllegalStateException("EventLib se está apagando.");
                    }
                    Connection connection = factory.newConnection(name + "-conn");
                    openConnections.add(connection);
                    connection.addShutdownListener(cause -> {
                        openConnections.remove(connection);
                        if (!cause.isInitiatedByApplication() && !closing.get()) {
                            log.warn("[eventlib] Conexión {} perdida; se reconectará.", name);
                            metrics.reconnect(type);
                            ref.set(build(type, ref));
                        }
                    });
                    return connection;
                })
                // newConnection es bloqueante: nunca en el event loop.
                .subscribeOn(Schedulers.boundedElastic())
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, properties.getReconnectDelay())
                        .filter(error -> !closing.get())
                        .doBeforeRetry(signal -> log.warn(
                                "[eventlib] Reintentando conexión {} (intento {}): {}",
                                name, signal.totalRetries() + 1,
                                signal.failure().getMessage())))
                .cache(
                        connection -> Duration.ofDays(365),
                        error -> Duration.ZERO,
                        () -> Duration.ZERO);
    }

    @Override
    public void destroy() {
        closing.set(true);
        int timeoutMillis = (int) Math.min(
                Integer.MAX_VALUE, properties.getShutdownTimeout().toMillis());
        for (Connection connection : openConnections) {
            try {
                if (connection.isOpen()) {
                    connection.close(timeoutMillis);
                }
            } catch (Exception error) {
                log.warn("[eventlib] No se pudo cerrar la conexión AMQP: {}",
                        error.getMessage());
            }
        }
        openConnections.clear();
        log.info("[eventlib] Conexiones RabbitMQ cerradas.");
    }
}
