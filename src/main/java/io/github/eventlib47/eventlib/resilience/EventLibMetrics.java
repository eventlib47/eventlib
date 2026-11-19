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

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Métricas de EventLib. Con {@code registry == null} todas las operaciones
 * son no-op, así que el código llamador nunca necesita comprobarlo.
 *
 * <ul>
 *   <li>{@code eventlib.resilience.retries{name}}</li>
 *   <li>{@code eventlib.resilience.failures{name,exception}}</li>
 *   <li>{@code eventlib.publish{outcome}}</li>
 *   <li>{@code eventlib.outbox.transitions{status}}</li>
 *   <li>{@code eventlib.outbox.pending} (gauge: mensajes pendientes)</li>
 *   <li>{@code eventlib.circuitbreaker.state{name}} (0 cerrado, 1 abierto, 2 semiabierto…)</li>
 *   <li>{@code eventlib.rabbit.reconnects{type}}</li>
 * </ul>
 */
public class EventLibMetrics {

    private final MeterRegistry registry;
    private final AtomicLong outboxPending = new AtomicLong();

    public EventLibMetrics(MeterRegistry registry) {
        this.registry = registry;
        if (registry != null) {
            Gauge.builder("eventlib.outbox.pending", outboxPending, AtomicLong::doubleValue)
                    .description("Eventos del Outbox aún sin publicar")
                    .register(registry);
        }
    }

    public static EventLibMetrics noop() {
        return new EventLibMetrics(null);
    }

    public void retry(String name) {
        if (registry != null) {
            registry.counter("eventlib.resilience.retries", "name", name).increment();
        }
    }

    public void failure(String name, Throwable error) {
        if (registry != null) {
            registry.counter("eventlib.resilience.failures",
                    "name", name, "exception", error.getClass().getSimpleName()).increment();
        }
    }

    public void published(String outcome) {
        if (registry != null) {
            registry.counter("eventlib.publish", "outcome", outcome).increment();
        }
    }

    public void outboxTransition(String status) {
        if (registry != null) {
            registry.counter("eventlib.outbox.transitions", "status", status).increment();
        }
    }

    public void outboxPending(long value) {
        outboxPending.set(value);
    }

    public void reconnect(String type) {
        if (registry != null) {
            registry.counter("eventlib.rabbit.reconnects", "type", type).increment();
        }
    }

    public void registerCircuitBreaker(String name, CircuitBreaker circuitBreaker) {
        if (registry != null) {
            Gauge.builder("eventlib.circuitbreaker.state", circuitBreaker,
                            cb -> cb.getState().getOrder())
                    .tag("name", name)
                    .register(registry);
        }
    }
}
