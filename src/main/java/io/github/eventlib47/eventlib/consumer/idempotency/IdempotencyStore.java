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
package io.github.eventlib47.eventlib.consumer.idempotency;

import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Almacén de idempotencia con semántica claim/renew/complete/release.
 *
 * <p>El {@code owner} es único POR ENTREGA (no por instancia) y actúa como
 * fencing token: solo el dueño vigente puede renovar, completar o liberar.</p>
 */
public interface IdempotencyStore {

    enum ClaimResult {
        /** El evento quedó reservado por esta entrega. */
        CLAIMED,
        /** Otra entrega lo está procesando. */
        IN_PROGRESS,
        /** Ya fue procesado correctamente; es un duplicado. */
        COMPLETED
    }

    /** Reserva atómica (equivalente a SET NX PX). */
    Mono<ClaimResult> tryClaim(String eventId, String owner, Duration ttl);

    /**
     * Extiende el TTL del claim mientras el handler sigue ejecutándose.
     * Devuelve {@code false} si el claim expiró o pertenece a otro owner
     * (claim perdido: la ejecución debe abortarse).
     *
     * <p>Por defecto devuelve {@code true} para no romper implementaciones
     * existentes; los stores reales deben sobrescribirlo.</p>
     */
    default Mono<Boolean> renewClaim(String eventId, String owner, Duration ttl) {
        return Mono.just(true);
    }

    /** Marca COMPLETED solo si {@code owner} sigue siendo el dueño. */
    Mono<Boolean> markCompleted(String eventId, String owner);

    /** Libera el claim solo si {@code owner} sigue siendo el dueño. */
    Mono<Boolean> releaseClaim(String eventId, String owner);
}
