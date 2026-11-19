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
package io.github.eventlib47.eventlib.producer.outbox;

import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.config.OutboxProperties;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Productor y publicador del Transactional Outbox.
 *
 * <p>Máquina de estados: {@code PENDING → PROCESSING → PUBLISHED}, o
 * {@code PROCESSING → FAILED → PROCESSING ...} y finalmente {@code DEAD}.</p>
 *
 * <p>Garantía: at-least-once hacia el broker. Si un worker pierde el lease
 * mientras publica, otro puede republicar: la idempotencia del consumidor
 * (Bloque 1) absorbe el duplicado.</p>
 */
@Slf4j
public class OutboxProcessor {

    private static final String TX_REQUIRED_MESSAGE =
            "OutboxEventPublisher.publish() requiere una transacción R2DBC activa "
                    + "(@Transactional o TransactionalOperator) para que el evento y la "
                    + "operación de negocio se confirmen juntos. Desactiva "
                    + "eventlib.infra.outbox.require-transaction solo si aceptas perder "
                    + "esa garantía.";

    /** Claim atómico con token por intento. Usa el reloj de la BD (sin desfase). */
    private static final String CLAIM_SQL =
            "UPDATE outbox_events "
                    + "SET status = 'PROCESSING', locked_by = :token, "
                    + "    locked_until = NOW() + make_interval(secs => :leaseSecs), "
                    + "    attempts = COALESCE(attempts, 0) + 1 "
                    + "WHERE id = :id AND published_at IS NULL "
                    + "AND (status IS NULL OR status = 'PENDING' "
                    + "     OR (status = 'FAILED' AND (next_attempt_at IS NULL OR next_attempt_at <= NOW())) "
                    + "     OR (status = 'PROCESSING' AND (locked_until IS NULL OR locked_until < NOW()))) "
                    + "RETURNING attempts";

    private static final String RENEW_SQL =
            "UPDATE outbox_events "
                    + "SET locked_until = NOW() + make_interval(secs => :leaseSecs) "
                    + "WHERE id = :id AND locked_by = :token "
                    + "AND status = 'PROCESSING' AND published_at IS NULL";

    private static final String PUBLISHED_SQL =
            "UPDATE outbox_events "
                    + "SET published_at = NOW(), status = 'PUBLISHED', "
                    + "    locked_by = NULL, locked_until = NULL, last_error = NULL "
                    + "WHERE id = :id AND locked_by = :token AND published_at IS NULL";

    private static final String FAILURE_SQL =
            "UPDATE outbox_events "
                    + "SET status = :status, locked_by = NULL, locked_until = NULL, "
                    + "    last_error = :error, "
                    + "    next_attempt_at = NOW() + make_interval(secs => :delaySecs) "
                    + "WHERE id = :id AND locked_by = :token AND published_at IS NULL";

    private final OutboxRepository repository;
    private final MessagePublisher publisher;
    private final MessageSerializer serializer;
    private final EventRoutingResolver routingResolver;
    private final InfraProperties infraProperties;
    private final DatabaseClient databaseClient;
    private final String owner;
    private final EventLibMetrics metrics;

    public OutboxProcessor(
            OutboxRepository repository,
            MessagePublisher publisher,
            MessageSerializer serializer,
            EventRoutingResolver routingResolver,
            InfraProperties infraProperties,
            DatabaseClient databaseClient,
            String owner) {
        this(repository, publisher, serializer, routingResolver, infraProperties,
                databaseClient, owner, EventLibMetrics.noop());
    }

    public OutboxProcessor(
            OutboxRepository repository,
            MessagePublisher publisher,
            MessageSerializer serializer,
            EventRoutingResolver routingResolver,
            InfraProperties infraProperties,
            DatabaseClient databaseClient,
            String owner,
            EventLibMetrics metrics) {
        this.repository = Objects.requireNonNull(repository, "repository no puede ser null");
        this.publisher = Objects.requireNonNull(publisher, "publisher no puede ser null");
        this.serializer = Objects.requireNonNull(serializer, "serializer no puede ser null");
        this.routingResolver = Objects.requireNonNull(
                routingResolver, "routingResolver no puede ser null");
        this.infraProperties = Objects.requireNonNull(
                infraProperties, "infraProperties no puede ser null");
        this.databaseClient = Objects.requireNonNull(
                databaseClient, "databaseClient no puede ser null");
        this.owner = Objects.requireNonNull(owner, "owner no puede ser null");
        this.metrics = metrics == null ? EventLibMetrics.noop() : metrics;
    }

    // ------------------------------------------------------------------
    // Lado productor: store() participa en la transacción del llamador
    // ------------------------------------------------------------------

    /**
     * Inserta el evento en {@code outbox_events}. Spring R2DBC enlaza el
     * INSERT a la transacción del contexto Reactor, así que se confirma o
     * revierte junto con la operación de negocio.
     */
    public Mono<Void> store(IntegrationEvent event) {
        Objects.requireNonNull(event, "event no puede ser null");

        return requireActiveTransaction()
                .then(Mono.fromCallable(() -> toOutboxEvent(event)))
                .flatMap(repository::save)
                .then();
    }

    private Mono<Void> requireActiveTransaction() {
        if (!outboxProperties().isRequireTransaction()) {
            return Mono.empty();
        }
        return TransactionSynchronizationManager.forCurrentTransaction()
                .filter(TransactionSynchronizationManager::isActualTransactionActive)
                .switchIfEmpty(Mono.error(new IllegalStateException(TX_REQUIRED_MESSAGE)))
                .onErrorMap(NoTransactionException.class,
                        error -> new IllegalStateException(TX_REQUIRED_MESSAGE, error))
                .then();
    }

    private OutboxEvent toOutboxEvent(IntegrationEvent event) {
        EventRoute route = routingResolver.resolve(event);
        String routingKey = route.exchangeType()
                == RabbitProperties.ExchangeType.X_CONSISTENT_HASH
                ? event.getAggregateId()
                : route.routingKey();

        return OutboxEvent.builder()
                .id(UUID.randomUUID())
                .eventId(event.getEventId())
                .eventName(event.getEventName())
                .topic(event.getEventName())
                .exchange(route.exchange())
                .routingKey(routingKey)
                .payload(serializer.serialize(event))
                .createdAt(Instant.now())
                .className(event.getClass().getName())
                .status(OutboxEvent.STATUS_PENDING)
                .attempts(0)
                .isUpdate(false)
                .build();
    }

    // ------------------------------------------------------------------
    // Lado publicador: claim → publicar con lease → marcar
    // ------------------------------------------------------------------

    public Mono<Void> process(OutboxEvent event) {
        if (event == null || event.getId() == null) {
            return Mono.error(new IllegalArgumentException(
                    "El evento de Outbox y su id son obligatorios."));
        }

        OutboxProperties properties = outboxProperties();
        // Token ÚNICO por intento: fencing frente a otros workers y a mis propios intentos viejos.
        String token = owner + ":" + UUID.randomUUID();

        // claim vacío = otra instancia lo reclamó primero.
        return claim(event.getId(), token, properties.getLockTtl())
                .flatMap(attempt -> {
                    if (attempt > properties.getMaxAttempts()) {
                        return markDead(event, token, attempt,
                                "Máximo de intentos alcanzado: " + properties.getMaxAttempts());
                    }

                    return publishWithLease(event, token, properties)
                            .then(Mono.defer(() -> markPublished(event.getId(), token)))
                            .flatMap(marked -> {
                                if (marked == 1) {
                                    metrics.outboxTransition("published");
                                    log.debug("Outbox {} publicado y marcado.", event.getId());
                                    return Mono.<Void>empty();
                                }
                                metrics.outboxTransition("lease_lost");
                                return Mono.<Void>error(new IllegalStateException(
                                        "Se perdió el lease del Outbox " + event.getId()));
                            })
                            .onErrorResume(error -> markFailed(event, token, attempt, error));
                });
    }

    /**
     * Publica mientras un heartbeat renueva el lease de la fila. Si el lease
     * se pierde, el heartbeat emite error y la publicación se cancela.
     */
    private Mono<Void> publishWithLease(
            OutboxEvent event, String token, OutboxProperties properties) {

        Duration interval = Duration.ofMillis(
                Math.max(100L, properties.getLockTtl().toMillis() / 3));

        Mono<Void> heartbeat = Flux.interval(interval)
                .concatMap(tick -> renewLease(event.getId(), token, properties.getLockTtl())
                        .onErrorResume(error -> {
                            log.warn("Fallo al renovar el lease del Outbox {}: {}",
                                    event.getId(), error.getMessage());
                            return Mono.just(1);
                        }))
                .filter(rows -> rows == 0)
                .next()
                .flatMap(lost -> Mono.<Void>error(new IllegalStateException(
                        "Se perdió el lease del Outbox " + event.getId()
                                + " durante la publicación.")));

        return Mono.firstWithSignal(publish(event), heartbeat);
    }

    private Mono<Integer> claim(UUID id, String token, Duration lease) {
        return databaseClient.sql(CLAIM_SQL)
                .bind("token", token)
                .bind("leaseSecs", seconds(lease))
                .bind("id", id)
                .map((row, metadata) -> row.get("attempts", Integer.class))
                .one();
    }

    private Mono<Integer> renewLease(UUID id, String token, Duration lease) {
        return databaseClient.sql(RENEW_SQL)
                .bind("leaseSecs", seconds(lease))
                .bind("id", id)
                .bind("token", token)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    private Mono<Integer> markPublished(UUID id, String token) {
        return databaseClient.sql(PUBLISHED_SQL)
                .bind("id", id)
                .bind("token", token)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    private Mono<Integer> updateFailure(
            UUID id, String token, String status, String error, Duration delay) {
        return databaseClient.sql(FAILURE_SQL)
                .bind("status", status)
                .bind("error", error)
                .bind("delaySecs", seconds(delay))
                .bind("id", id)
                .bind("token", token)
                .fetch()
                .rowsUpdated()
                .map(Long::intValue);
    }

    private Mono<Void> publish(OutboxEvent event) {
        String exchange = event.getExchange();
        if (exchange == null || exchange.isBlank()) {
            exchange = infraProperties.getRabbitmq().getExchange();
        }
        String routingKey = event.getRoutingKey();
        if (routingKey == null || routingKey.isBlank()) {
            routingKey = event.getTopic();
        }
        if (exchange == null || exchange.isBlank()
                || routingKey == null || routingKey.isBlank()) {
            return Mono.error(new IllegalStateException(
                    "El evento de Outbox " + event.getId()
                            + " no tiene exchange/routing key."));
        }
        if (event.getPayload() == null) {
            return Mono.error(new IllegalStateException(
                    "El evento de Outbox " + event.getId() + " no tiene payload."));
        }

        return publisher.publish(
                exchange,
                routingKey,
                event.getPayload().getBytes(StandardCharsets.UTF_8));
    }

    private Mono<Void> markFailed(
            OutboxEvent event, String token, int attempt, Throwable error) {
        OutboxProperties properties = outboxProperties();
        String message = error.getMessage() == null
                ? error.getClass().getName()
                : error.getMessage();

        // Intentos agotados: DEAD inmediato (no espera otro ciclo de claim).
        boolean exhausted = attempt >= properties.getMaxAttempts();
        String status = exhausted ? OutboxEvent.STATUS_DEAD : OutboxEvent.STATUS_FAILED;
        Duration delay = exhausted ? Duration.ZERO : backoff(properties, attempt);

        log.warn("Outbox {} falló en intento {}/{} -> {}: {}",
                event.getId(), attempt, properties.getMaxAttempts(), status, message);

        return updateFailure(event.getId(), token, status, truncate(message), delay)
                .doOnNext(updated -> {
                    if (updated == 0) {
                        log.warn("No se pudo marcar {} el Outbox {}; lease perdido.",
                                status, event.getId());
                    } else {
                        metrics.outboxTransition(exhausted ? "dead" : "failed");
                    }
                })
                .then();
    }

    private Mono<Void> markDead(OutboxEvent event, String token, int attempt, String reason) {
        log.error("Outbox {} pasó a DEAD tras {} intentos: {}",
                event.getId(), attempt, reason);

        return updateFailure(
                event.getId(), token, OutboxEvent.STATUS_DEAD, truncate(reason), Duration.ZERO)
                .doOnNext(updated -> {
                    if (updated > 0) {
                        metrics.outboxTransition("dead");
                    }
                })
                .then();
    }

    private OutboxProperties outboxProperties() {
        return infraProperties.getOutbox() != null
                ? infraProperties.getOutbox()
                : new OutboxProperties();
    }

    /** Exponencial con tope y jitter ±20 % para evitar tormentas de reintentos. */
    private static Duration backoff(OutboxProperties properties, int attempt) {
        Duration base = properties.getRetryBackoff() == null
                ? Duration.ofSeconds(3)
                : properties.getRetryBackoff();
        Duration max = properties.getMaxBackoff() == null
                ? Duration.ofMinutes(5)
                : properties.getMaxBackoff();

        long multiplier = 1L << Math.min(Math.max(attempt - 1, 0), 10);
        Duration delay = base.multipliedBy(multiplier);
        if (delay.compareTo(max) > 0) {
            delay = max;
        }
        double jitter = 0.8 + ThreadLocalRandom.current().nextDouble() * 0.4;
        return Duration.ofMillis((long) (delay.toMillis() * jitter));
    }

    private static double seconds(Duration duration) {
        return duration.toMillis() / 1000.0;
    }

    private static String truncate(String value) {
        if (value == null) return null;
        return value.length() <= 2000 ? value : value.substring(0, 2000);
    }
}
