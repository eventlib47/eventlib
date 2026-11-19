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
package io.github.eventlib47.eventlib.core;

import java.time.Instant;
import java.util.UUID;

/**
 * Metadata estándar para eventos de integración.
 */
public class EventMetadata {

    private String eventId;
    private Instant occurredAt;
    private String source;

    /**
     * Clave de agregado, usada como hash key cuando el evento se publica en
     * un exchange x-consistent-hash (ver RabbitProperties.ExchangeType).
     * Nullable: si no se setea explícitamente, IntegrationEvent.getAggregateId()
     * cae de vuelta al eventId — es decir, sin garantía real de orden entre
     * eventos distintos (mismo comportamiento que hoy).
     */
    private String aggregateId;

    public EventMetadata() {
        this.eventId = UUID.randomUUID().toString();
        this.occurredAt = Instant.now();
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public void setOccurredAt(Instant occurredAt) {
        this.occurredAt = occurredAt;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public void setAggregateId(String aggregateId) {
        this.aggregateId = aggregateId;
    }
}
