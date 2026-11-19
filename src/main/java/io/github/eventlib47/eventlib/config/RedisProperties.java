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
 * Configuración Redis — SOLO se usa si Spring Boot no creó ya su propio
 * RedisConnectionFactory a partir de spring.data.redis.* (el prefijo
 * estándar). eventlib.infra.redis.host/port/password/ssl-enabled son un
 * fallback explícito, no la fuente de verdad por defecto.
 */
public class RedisProperties {

    private String host = "localhost";
    private int port = 6379;
    private String password;

    /**
     * Habilita SSL/TLS en la conexión a Redis cuando este fallback
     * es el que efectivamente se usa (Spring Boot no tenía spring.data.redis.*).
     */
    private boolean sslEnabled = false;

    private String idempotencyPrefix = "eventlib:idempotency";

    /**
     * TTL del estado COMPLETED. Pasado este tiempo un evento podría volver
     * a procesarse si el broker lo reentrega.
     */
    private Duration idempotencyTtl = Duration.ofHours(24);

    /**
     * TTL del claim IN_PROGRESS. Debe cubrir el peor caso del handler.
     */
    private Duration claimTtl = Duration.ofSeconds(30);

    /**
     * Solo para pruebas locales. En producción el store nulo está prohibido
     * porque desactiva la idempotencia sin avisar al operador.
     */
    private boolean allowNoOp = false;

    public String getHost() { return host; }
    public void setHost(String host) { this.host = host; }

    public int getPort() { return port; }
    public void setPort(int port) { this.port = port; }

    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public boolean isSslEnabled() { return sslEnabled; }
    public void setSslEnabled(boolean sslEnabled) { this.sslEnabled = sslEnabled; }

    public String getIdempotencyPrefix() { return idempotencyPrefix; }
    public void setIdempotencyPrefix(String idempotencyPrefix) { this.idempotencyPrefix = idempotencyPrefix; }

    public Duration getIdempotencyTtl() { return idempotencyTtl; }
    public void setIdempotencyTtl(Duration idempotencyTtl) {
        this.idempotencyTtl = idempotencyTtl;
    }

    public Duration getClaimTtl() { return claimTtl; }
    public void setClaimTtl(Duration claimTtl) { this.claimTtl = claimTtl; }

    public boolean isAllowNoOp() { return allowNoOp; }
    public void setAllowNoOp(boolean allowNoOp) { this.allowNoOp = allowNoOp; }
}