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
package io.github.eventlib47.eventlib.producer;

import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Function;

/**
 * Publicador transaccional: escribe el evento en el Outbox.
 */
public class OutboxEventPublisher implements EventPublisher {

    private final OutboxProcessor outboxProcessor;
    private final TransactionalOperator transactionalOperator;

    public OutboxEventPublisher(OutboxProcessor outboxProcessor) {
        this(outboxProcessor, null);
    }

    public OutboxEventPublisher(
            OutboxProcessor outboxProcessor,
            TransactionalOperator transactionalOperator) {
        this.outboxProcessor = Objects.requireNonNull(outboxProcessor, "outboxProcessor");
        this.transactionalOperator = transactionalOperator;
    }

    /** Debe invocarse dentro de una transacción R2DBC (ver require-transaction). */
    @Override
    public Mono<Void> publish(IntegrationEvent event) {
        return outboxProcessor.store(event);
    }

    /**
     * Ejecuta la operación de negocio y guarda el evento derivado en UNA sola
     * transacción: si cualquiera falla, ambas se revierten.
     *
     * <pre>{@code
     * outboxPublisher.executeAndPublish(
     *         userRepository.save(user),
     *         saved -> new UserCreatedEvent(saved.getId()));
     * }</pre>
     *
     * Si la operación de negocio no emite valor, no se crea evento.
     */
    public <T> Mono<T> executeAndPublish(
            Mono<T> businessOperation,
            Function<? super T, ? extends IntegrationEvent> eventFactory) {
        if (transactionalOperator == null) {
            return Mono.error(new IllegalStateException(
                    "No hay TransactionalOperator disponible; configura un "
                            + "ReactiveTransactionManager R2DBC o usa @Transactional."));
        }
        Mono<T> work = businessOperation.flatMap(result ->
                outboxProcessor.store(eventFactory.apply(result)).thenReturn(result));
        return transactionalOperator.transactional(work);
    }
}
