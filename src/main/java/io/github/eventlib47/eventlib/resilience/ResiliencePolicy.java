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
package io.github.eventlib47.eventlib.resilience;

import com.rabbitmq.client.AlreadyClosedException;
import io.github.eventlib47.eventlib.broker.MessagePublishException;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

/**
 * Política centralizada de resiliencia.
 *
 * <pre>
 * retry( circuitBreaker( bulkhead( timeout( operación ) ) ) )
 * </pre>
 *
 * <p>El timeout es POR INTENTO; el circuit breaker cuenta cada intento; el
 * retry solo repite errores transitorios (nunca breaker abierto, bulkhead
 * lleno ni mensajes no enrutables).</p>
 */
public class ResiliencePolicy {

    private final String name;
    private final ResilienceProperties properties;
    private final EventLibMetrics metrics;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;

    public ResiliencePolicy(String name, ResilienceProperties properties, EventLibMetrics metrics) {
        this.name = Objects.requireNonNull(name, "name no puede ser null");
        this.properties = properties == null ? new ResilienceProperties() : properties;
        this.metrics = metrics == null ? EventLibMetrics.noop() : metrics;
        this.circuitBreaker = this.properties.getCircuitBreaker().isEnabled()
                ? newCircuitBreaker(name, this.properties) : null;
        this.bulkhead = this.properties.getBulkhead().isEnabled()
                ? newBulkhead(name, this.properties) : null;
        if (this.circuitBreaker != null) {
            this.metrics.registerCircuitBreaker(name, this.circuitBreaker);
        }
    }

    /** Fábrica reutilizable (p. ej. por el proveedor de conexiones). */
    public static CircuitBreaker newCircuitBreaker(String name, ResilienceProperties props) {
        ResilienceProperties.CircuitBreakerProperties cb = props.getCircuitBreaker();
        return CircuitBreaker.of(name, CircuitBreakerConfig.custom()
                .failureRateThreshold(cb.getFailureRateThreshold())
                .slidingWindowSize(cb.getSlidingWindowSize())
                .minimumNumberOfCalls(cb.getMinimumNumberOfCalls())
                .waitDurationInOpenState(cb.getWaitDurationInOpenState())
                .permittedNumberOfCallsInHalfOpenState(cb.getPermittedCallsInHalfOpenState())
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .recordException(ResiliencePolicy::countsAsFailure)
                .build());
    }

    public static Bulkhead newBulkhead(String name, ResilienceProperties props) {
        ResilienceProperties.BulkheadProperties bh = props.getBulkhead();
        return Bulkhead.of(name, BulkheadConfig.custom()
                .maxConcurrentCalls(bh.getMaxConcurrentCalls())
                .maxWaitDuration(bh.getMaxWaitDuration())
                .build());
    }

    public EventLibMetrics metrics() {
        return metrics;
    }

    public CircuitBreaker circuitBreaker() {
        return circuitBreaker;
    }

    /**
     * Ejecuta {@code operation} bajo la política. El supplier se invoca en
     * cada intento, de modo que cada reintento reconstruye el flujo.
     */
    public <T> Mono<T> protect(Supplier<Mono<T>> operation) {
        Mono<T> attempt = Mono.defer(operation)
                .timeout(properties.getTimeout());

        if (bulkhead != null) {
            attempt = attempt.transformDeferred(BulkheadOperator.of(bulkhead));
        }
        if (circuitBreaker != null) {
            attempt = attempt.transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
        }

        ResilienceProperties.RetryProperties retry = properties.getRetry();
        long retries = Math.max(0, retry.getMaxAttempts() - 1);
        if (retries > 0) {
            attempt = attempt.retryWhen(Retry.backoff(retries, retry.getBackoff())
                    .maxBackoff(retry.getMaxBackoff())
                    .jitter(retry.getJitter())
                    .filter(ResiliencePolicy::isRetryable)
                    .doBeforeRetry(signal -> metrics.retry(name))
                    // Conserva el error original en lugar de RetryExhaustedException.
                    .onRetryExhaustedThrow((spec, signal) -> signal.failure()));
        }

        return attempt.doOnError(error -> metrics.failure(name, error));
    }

    static boolean isRetryable(Throwable error) {
        if (error instanceof CallNotPermittedException || error instanceof BulkheadFullException) {
            return false;
        }
        if (error instanceof TimeoutException) {
            return true;
        }
        if (error instanceof MessagePublishException publishError) {
            return publishError.isRetryable();
        }
        if (error instanceof IllegalArgumentException) {
            return false;
        }
        if (error instanceof IOException || error instanceof AlreadyClosedException) {
            return true;
        }
        String message = error.getMessage();
        return message != null && message.toLowerCase(Locale.ROOT).contains("connection");
    }

    /** Errores de configuración/topología no deben abrir el circuit breaker. */
    private static boolean countsAsFailure(Throwable error) {
        if (error instanceof IllegalArgumentException) {
            return false;
        }
        if (error instanceof MessagePublishException publishError) {
            return publishError.isRetryable();
        }
        return true;
    }
}
