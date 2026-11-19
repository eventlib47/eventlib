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
package io.github.eventlib47.eventlib.consumer;

import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Despacha eventos a su handler aplicando idempotencia fail-closed.
 *
 * <p>Cada entrega genera su propio {@code owner} ({@code instanceId:UUID}). Así
 * dos entregas simultáneas del mismo eventId —incluso en la misma instancia—
 * compiten de verdad por el claim y solo una ejecuta el handler.</p>
 *
 * <p>Garantías de esta versión:</p>
 * <ul>
 *   <li>Orden estricto handler → markCompleted (fenced) → ACK (en el receiver).</li>
 *   <li>El claim se libera si el handler falla Y si la suscripción se cancela
 *       en cualquier punto (incluso mientras el claim todavía está en vuelo).
 *       La liberación está protegida por el owner: nunca borra un estado
 *       COMPLETED ni el claim de otra entrega.</li>
 *   <li>Si Redis no puede renovar el claim durante un TTL completo, el claim
 *       se da por perdido y el handler se cancela, porque ya no hay exclusión
 *       mutua garantizada.</li>
 * </ul>
 */
@Slf4j
public class EventDispatcher {

    private final Map<String, IntegrationEventHandler<IntegrationEvent>> handlers;
    private final IdempotencyStore idempotencyStore;
    private final String instanceId;
    private final Duration claimTtl;
    private final Duration heartbeatInterval;

    public EventDispatcher(
            List<IntegrationEventHandler<?>> handlerList,
            IdempotencyStore idempotencyStore) {
        this(handlerList, idempotencyStore,
                "eventlib-" + UUID.randomUUID(), Duration.ofSeconds(30));
    }

    /**
     * @param instanceId identificador de la instancia; se usa como prefijo del
     *                   owner por entrega
     * @param claimTtl   TTL del claim; el heartbeat lo renueva cada TTL/3
     */
    public EventDispatcher(
            List<IntegrationEventHandler<?>> handlerList,
            IdempotencyStore idempotencyStore,
            String instanceId,
            Duration claimTtl) {

        Objects.requireNonNull(idempotencyStore,
                "IdempotencyStore es obligatorio; configura Redis o habilita "
                        + "eventlib.infra.redis.allow-no-op=true solo en pruebas.");
        this.idempotencyStore = idempotencyStore;
        this.instanceId = requireText(instanceId, "instanceId");
        this.claimTtl = requirePositive(claimTtl, "claimTtl");
        this.heartbeatInterval = Duration.ofMillis(Math.max(10L, claimTtl.toMillis() / 3));
        this.handlers = indexHandlers(handlerList);
    }

    /**
     * Procesa una entrega. El {@code Mono} completa vacío solo si el evento
     * quedó COMPLETED (o ya lo estaba); cualquier otro desenlace es error.
     */
    public Mono<Void> dispatch(IntegrationEvent event) {
        return Mono.defer(() -> {
            if (event == null) {
                return Mono.error(new PermanentEventException(
                        "El evento no puede ser null."));
            }

            String eventId = requireText(event.getEventId(), "eventId");
            String eventName = requireText(event.getEventName(), "eventName");

            // Owner nuevo por suscripción: también en cada reintento del Mono.
            String owner = instanceId + ":" + UUID.randomUUID();

            return idempotencyStore.tryClaim(eventId, owner, claimTtl)
                    .switchIfEmpty(Mono.error(new TransientEventException(
                            "IdempotencyStore no devolvió resultado para " + eventId)))
                    .onErrorMap(error -> isProcessingError(error)
                            ? error
                            : new TransientEventException(
                            "No se pudo reclamar el evento " + eventId, error))
                    .flatMap(claim -> handleClaim(event, eventId, eventName, owner, claim))
                    // Cancelación en CUALQUIER punto (apagado, timeout aguas arriba):
                    // onErrorResume no se ejecuta ante un cancel, y el claim puede
                    // haberse reservado en Redis sin que su resultado llegara aún
                    // aquí. La liberación es fenced por owner, así que es inocua si
                    // el claim no existe, ya es COMPLETED o pertenece a otra entrega.
                    .doOnCancel(() -> releaseOnCancel(eventId, owner));
        });
    }

    private Mono<Void> handleClaim(
            IntegrationEvent event,
            String eventId,
            String eventName,
            String owner,
            IdempotencyStore.ClaimResult claim) {

        if (claim == IdempotencyStore.ClaimResult.COMPLETED) {
            log.debug("Evento duplicado ya completado: {}", eventId);
            return Mono.empty();
        }

        if (claim == IdempotencyStore.ClaimResult.IN_PROGRESS) {
            return Mono.error(new TransientEventException(
                    "El evento " + eventId + " está siendo procesado por otra entrega."));
        }

        IntegrationEventHandler<IntegrationEvent> handler = handlers.get(eventName);
        if (handler == null) {
            // Error de configuración permanente: liberar y mandar a DLQ.
            return releaseQuietly(eventId, owner)
                    .then(Mono.<Void>error(new PermanentEventException(
                            "No existe handler para el evento " + eventName)));
        }

        log.debug("Despachando evento {} ({}) owner={}", eventId, eventName, owner);

        // Instante de la última evidencia de que el claim seguía vivo. Se crea
        // por entrega (handleClaim se invoca dentro del defer de dispatch).
        AtomicLong lastRenewalNanos = new AtomicLong(System.nanoTime());

        // El handler compite con el heartbeat: si el claim se pierde, el
        // heartbeat emite error y firstWithSignal CANCELA el handler. Si el
        // handler termina primero, el heartbeat se cancela.
        Mono<Void> guarded = Mono.firstWithSignal(
                Mono.defer(() -> handler.handle(event)),
                heartbeat(eventId, owner, lastRenewalNanos));

        // Orden estricto: handler → markCompleted (fenced) → ACK (en el receiver).
        return guarded
                .then(Mono.defer(() -> idempotencyStore.markCompleted(eventId, owner)))
                .flatMap(completed -> {
                    if (Boolean.TRUE.equals(completed)) {
                        log.debug("Evento completado y confirmado en idempotencia: {}", eventId);
                        return Mono.<Void>empty();
                    }
                    return Mono.<Void>error(new TransientEventException(
                            "El claim del evento " + eventId
                                    + " expiró o pertenece a otra entrega."));
                })
                .onErrorResume(error -> releaseQuietly(eventId, owner)
                        .then(Mono.<Void>error(error)));
    }

    /**
     * Renueva el claim cada TTL/3. Nunca completa mientras el claim siga vivo;
     * emite {@link TransientEventException} cuando Redis confirma que se perdió
     * o cuando no se ha podido renovar durante un TTL completo (el claim ya
     * habrá expirado en Redis aunque no podamos confirmarlo).
     *
     * <p>Un fallo aislado de infraestructura se tolera: el siguiente latido
     * puede recuperarlo antes de que venza el TTL.</p>
     */
    private Mono<Void> heartbeat(String eventId, String owner, AtomicLong lastRenewalNanos) {
        return Flux.interval(heartbeatInterval)
                .concatMap(tick -> idempotencyStore.renewClaim(eventId, owner, claimTtl)
                        .doOnNext(renewed -> {
                            if (Boolean.TRUE.equals(renewed)) {
                                lastRenewalNanos.set(System.nanoTime());
                            }
                        })
                        .onErrorResume(error -> {
                            long silentNanos = System.nanoTime() - lastRenewalNanos.get();
                            boolean claimStillValid = silentNanos < claimTtl.toNanos();
                            log.warn("Fallo al renovar el claim de {} ({} ms sin renovar, ttl={} ms): {}",
                                    eventId,
                                    Duration.ofNanos(silentNanos).toMillis(),
                                    claimTtl.toMillis(),
                                    error.getMessage());
                            // true = tolerar; false = tratar como claim perdido.
                            return Mono.just(claimStillValid);
                        }))
                .filter(renewed -> !renewed)
                .next()
                .flatMap(lost -> Mono.<Void>error(new TransientEventException(
                        "Se perdió el claim del evento " + eventId + " durante el proceso.")));
    }

    /** Liberación best-effort: el error original siempre prevalece. */
    private Mono<Void> releaseQuietly(String eventId, String owner) {
        return idempotencyStore.releaseClaim(eventId, owner)
                .onErrorResume(releaseError -> {
                    log.error("No se pudo liberar el claim de {}: {}",
                            eventId, releaseError.getMessage());
                    return Mono.just(false);
                })
                .then();
    }

    /**
     * Liberación disparada por cancelación. Se suscribe de forma independiente
     * porque la cadena original ya fue cancelada.
     */
    private void releaseOnCancel(String eventId, String owner) {
        log.debug("Entrega cancelada; liberando claim de {} owner={}", eventId, owner);
        releaseQuietly(eventId, owner).subscribe();
    }

    private static Map<String, IntegrationEventHandler<IntegrationEvent>> indexHandlers(
            List<IntegrationEventHandler<?>> handlerList) {

        Map<String, IntegrationEventHandler<IntegrationEvent>> indexed = new LinkedHashMap<>();
        if (handlerList == null) {
            return indexed;
        }

        for (IntegrationEventHandler<?> handler : handlerList) {
            if (handler == null) {
                continue;
            }
            String eventName = requireText(handler.getEventName(), "handler.getEventName()");
            @SuppressWarnings("unchecked")
            IntegrationEventHandler<IntegrationEvent> typed =
                    (IntegrationEventHandler<IntegrationEvent>) handler;
            if (indexed.putIfAbsent(eventName, typed) != null) {
                throw new IllegalStateException(
                        "Hay más de un handler registrado para el evento " + eventName);
            }
        }
        return Map.copyOf(indexed);
    }

    private static boolean isProcessingError(Throwable error) {
        return error instanceof EventProcessingException;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new PermanentEventException(field + " no puede ser null ni estar vacío.");
        }
        return value.trim();
    }

    private static Duration requirePositive(Duration value, String field) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(field + " debe ser mayor que cero.");
        }
        return value;
    }
}