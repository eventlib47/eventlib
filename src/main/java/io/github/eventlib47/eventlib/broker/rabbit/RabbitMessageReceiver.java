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

import io.github.eventlib47.eventlib.broker.MessageReceiver;
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.EventProcessingException;
import io.github.eventlib47.eventlib.consumer.PermanentEventException;
import io.github.eventlib47.eventlib.consumer.TransientEventException;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.rabbitmq.ConsumeOptions;
import reactor.rabbitmq.Receiver;
import reactor.util.retry.Retry;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * Implementación de {@link MessageReceiver} para RabbitMQ.
 *
 * <p>Reglas de ACK:</p>
 * <ul>
 *   <li>Procesado o duplicado completado: ACK.</li>
 *   <li>Error permanente: NACK sin requeue (DLQ).</li>
 *   <li>Error transitorio o {@code IN_PROGRESS}: NACK con requeue.</li>
 *   <li>Error de deserialización: NACK sin requeue.</li>
 * </ul>
 */
@Slf4j
public class RabbitMessageReceiver implements MessageReceiver {

    private final Receiver receiver;
    private final MessageSerializer serializer;
    private final EventDispatcher dispatcher;
    private final Duration retryDelay;
    private final int prefetch;

    public RabbitMessageReceiver(
            Receiver receiver,
            MessageSerializer serializer,
            EventDispatcher dispatcher) {
        this(receiver, serializer, dispatcher, Duration.ofSeconds(2), 1);
    }

    public RabbitMessageReceiver(
            Receiver receiver,
            MessageSerializer serializer,
            EventDispatcher dispatcher,
            Duration retryDelay) {
        this(receiver, serializer, dispatcher, retryDelay, 1);
    }

    public RabbitMessageReceiver(
            Receiver receiver,
            MessageSerializer serializer,
            EventDispatcher dispatcher,
            Duration retryDelay,
            int prefetch) {
        this.receiver = receiver;
        this.serializer = serializer;
        this.dispatcher = dispatcher;
        this.retryDelay = retryDelay == null || retryDelay.isNegative()
                ? Duration.ofSeconds(2)
                : retryDelay;
        this.prefetch = Math.max(1, prefetch);
    }

    @Override
    public <T extends IntegrationEvent> Flux<T> consume(
            String queueName, Class<T> eventType) {

        log.info("[eventlib] Iniciando consumo en cola: {}", queueName);

        return Flux.defer(() -> {
                    Sinks.Many<T> errorTrigger = Sinks.many()
                            .unicast()
                            .onBackpressureBuffer();

                    ConsumeOptions options = new ConsumeOptions().qos(prefetch);
                    Flux<T> consumeFlux = receiver.consumeManualAck(queueName, options)
                            // concatMap (no parallel/flatMap): un solo handler
                            // en vuelo por receiver; prefetch=1 por defecto.
                            .concatMap(delivery -> consumeOne(queueName, eventType, delivery))
                            .doOnComplete(() -> {
                                log.warn("[{}] Flux de consumo completado; forzando reconexión.",
                                        queueName);
                                errorTrigger.tryEmitError(new IllegalStateException(
                                        "Consumer flux completed unexpectedly on queue: "
                                                + queueName));
                            });

                    return Flux.merge(consumeFlux, errorTrigger.asFlux());
                })
                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofSeconds(5))
                        .doBeforeRetry(signal -> log.warn(
                                "[{}] Reconectando consumidor (intento {}).",
                                queueName, signal.totalRetries() + 1)))
                .doOnError(error -> log.error(
                        "Error crítico en consumidor de {}: {}",
                        queueName, error.getMessage()));
    }

    <T extends IntegrationEvent> Mono<T> consumeOne(
            String queueName,
            Class<T> eventType,
            reactor.rabbitmq.AcknowledgableDelivery delivery) {

        final T event;
        try {
            String payload = new String(delivery.getBody(), StandardCharsets.UTF_8);
            event = serializer.deserialize(payload, eventType);
        } catch (Exception error) {
            log.error("[{}] Error de deserialización; enviando a DLQ: {}",
                    queueName, error.getMessage());
            safeNack(delivery, false);
            return Mono.empty();
        }

        // El ACK queda bajo control del flujo: solo se emite si dispatch
        // terminó (handler exitoso + markCompleted, o duplicado COMPLETED).
        return dispatcher.dispatch(event)
                .doOnSuccess(ignored -> {
                    safeAck(delivery);
                    log.debug("[eventlib] ACK evento {} en cola {}",
                            event.getEventId(), queueName);
                })
                .onErrorResume(error -> classifyAndNack(queueName, event, delivery, error))
                .thenReturn(event);
    }

    private Mono<Void> classifyAndNack(
            String queueName,
            IntegrationEvent event,
            reactor.rabbitmq.AcknowledgableDelivery delivery,
            Throwable error) {

        if (error instanceof PermanentEventException) {
            log.error("[{}] Error permanente en evento {}; DLQ: {}",
                    queueName, event.getEventId(), error.getMessage());
            safeNack(delivery, false);
            return Mono.empty();
        }

        if (error instanceof TransientEventException) {
            log.warn("[{}] Error transitorio en evento {}; requeue en {} ms: {}",
                    queueName, event.getEventId(), retryDelay.toMillis(),
                    error.getMessage());
            return safeNackAfterDelay(delivery, true);
        }

        if (error instanceof EventProcessingException) {
            log.error("[{}] Error de procesamiento en evento {}; DLQ: {}",
                    queueName, event.getEventId(), error.getMessage());
            safeNack(delivery, false);
            return Mono.empty();
        }

        // Un handler puede fallar por DB/red. Reencolar es más seguro que
        // perder el evento; los errores de negocio deben lanzar
        // PermanentEventException explícitamente.
        log.warn("[{}] Error no clasificado en evento {}; requeue: {}",
                queueName, event.getEventId(), error.getMessage());
        return safeNackAfterDelay(delivery, true);
    }

    private Mono<Void> safeNackAfterDelay(
            reactor.rabbitmq.AcknowledgableDelivery delivery, boolean requeue) {
        // Mono.delay: backoff no bloqueante (scheduler paralelo de Reactor),
        // sin ocupar hilos del event loop.
        return Mono.delay(retryDelay)
                .then(Mono.<Void>fromRunnable(() -> safeNack(delivery, requeue)))
                .onErrorResume(error -> {
                    log.error("No se pudo reencolar el delivery: {}", error.getMessage());
                    return Mono.<Void>empty();
                });
    }

    private void safeAck(reactor.rabbitmq.AcknowledgableDelivery delivery) {
        try {
            delivery.ack();
        } catch (Exception error) {
            log.error("No se pudo confirmar el delivery: {}", error.getMessage());
        }
    }

    private void safeNack(reactor.rabbitmq.AcknowledgableDelivery delivery, boolean requeue) {
        try {
            delivery.nack(requeue);
        } catch (Exception error) {
            log.error("No se pudo rechazar el delivery: {}", error.getMessage());
        }
    }
}