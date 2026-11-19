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

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Evento base de integración.
 */
public abstract class IntegrationEvent {

    private final EventMetadata metadata = new EventMetadata();

    public EventMetadata getMetadata() {
        return metadata;
    }

    /**
     * Nombre lógico del evento.
     *
     * Ej: user-created
     */
    public abstract String getEventName();

    /**
     * Este método permite que el EventDispatcher acceda al ID
     * sin conocer los detalles internos de EventMetadata.
     */
    @JsonIgnore
    public String getEventId() {
        return metadata.getEventId();
    }

    /**
     * Clave que determina el orden de procesamiento garantizado cuando el
     * evento se publica en un exchange x-consistent-hash: todos los
     * eventos con el mismo aggregateId caen siempre en el mismo shard.
     * Si el evento concreto no llama a setAggregateId(...), cae de vuelta
     * al eventId — es decir, sin garantía real de orden entre eventos
     * distintos (comportamiento idéntico al actual).
     */
    @JsonIgnore
    public String getAggregateId() {
        String aggregateId = metadata.getAggregateId();
        return aggregateId != null ? aggregateId : getEventId();
    }

    /**
     * Llamar desde el constructor del evento concreto, ej:
     * setAggregateId(userId) en UserRegisteredEvent.
     */
    protected void setAggregateId(String aggregateId) {
        metadata.setAggregateId(aggregateId);
    }

}
