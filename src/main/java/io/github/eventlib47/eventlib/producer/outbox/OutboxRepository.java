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

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Lectura de filas candidatas. El CLAIM atómico y las transiciones de estado
 * viven en {@link OutboxProcessor} (UPDATE ... RETURNING con token por intento).
 */
public interface OutboxRepository extends ReactiveCrudRepository<OutboxEvent, UUID> {

    /**
     * Candidatas: nuevas, fallidas cuyo backoff venció, o en PROCESSING con
     * lease vencido (recuperación tras caída). El orden preserva la antigüedad.
     */
    @Query("SELECT * FROM outbox_events "
            + "WHERE published_at IS NULL "
            + "AND (status IS NULL OR status IN ('PENDING', 'FAILED') "
            + "     OR (status = 'PROCESSING' AND (locked_until IS NULL OR locked_until < NOW()))) "
            + "AND (next_attempt_at IS NULL OR next_attempt_at <= NOW()) "
            + "ORDER BY created_at ASC "
            + "LIMIT :limit")
    Flux<OutboxEvent> findPending(@Param("limit") int limit);

    /** Mensajes pendientes de publicar (excluye DEAD). Alimenta el gauge. */
    @Query("SELECT COUNT(*) FROM outbox_events "
            + "WHERE published_at IS NULL AND (status IS NULL OR status <> 'DEAD')")
    Mono<Long> countPending();
}
