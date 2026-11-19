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
package io.github.eventlib47.eventlib.config;

import java.time.Duration;

/**
 * Configuración declarativa del patrón Transactional Outbox.
 *
 * eventlib.infra.outbox.*
 */
public class OutboxProperties {

    /** Activa el patrón Outbox. Si es false, EventPublisher publica directo al broker. */
    private boolean enabled = false;

    /** Máximo de filas que procesa el scheduler por ciclo. */
    private int batchSize = 100;

    /** Intervalo de polling del OutboxScheduler, en milisegundos. */
    private long pollingInterval = 5000;

    /** Solo una réplica procesa el Outbox por ciclo (lock líder en Redis). */
    private boolean lockEnabled = true;

    /** TTL del lease del lote y de cada fila reclamada. Se renueva cada TTL/3. */
    private Duration lockTtl = Duration.ofSeconds(60);

    /** Intentos máximos antes de pasar a DEAD. */
    private int maxAttempts = 5;

    /** Backoff base entre reintentos (exponencial con jitter). */
    private Duration retryBackoff = Duration.ofSeconds(3);

    /** Tope del backoff exponencial. */
    private Duration maxBackoff = Duration.ofMinutes(5);

    /**
     * Si es true (recomendado), {@code store()} falla cuando no hay una
     * transacción R2DBC activa: garantiza que el evento y la operación de
     * negocio se confirman o revierten juntos.
     */
    private boolean requireTransaction = true;

    public boolean isEnabled() { return enabled; }
    public void setEnabled(boolean enabled) { this.enabled = enabled; }

    public int getBatchSize() { return batchSize; }
    public void setBatchSize(int batchSize) { this.batchSize = batchSize; }

    public long getPollingInterval() { return pollingInterval; }
    public void setPollingInterval(long pollingInterval) { this.pollingInterval = pollingInterval; }

    public boolean isLockEnabled() { return lockEnabled; }
    public void setLockEnabled(boolean lockEnabled) { this.lockEnabled = lockEnabled; }

    public Duration getLockTtl() { return lockTtl; }
    public void setLockTtl(Duration lockTtl) { this.lockTtl = lockTtl; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public Duration getRetryBackoff() { return retryBackoff; }
    public void setRetryBackoff(Duration retryBackoff) { this.retryBackoff = retryBackoff; }

    public Duration getMaxBackoff() { return maxBackoff; }
    public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }

    public boolean isRequireTransaction() { return requireTransaction; }
    public void setRequireTransaction(boolean requireTransaction) {
        this.requireTransaction = requireTransaction;
    }
}
