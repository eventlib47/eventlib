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
package io.github.eventlib47.eventlib.broker;

/**
 * Fallo de publicación al broker con causa tipificada.
 */
public class MessagePublishException extends IllegalStateException {

    public enum Reason {
        /** El exchange no tiene cola vinculada (basic.return). No es reintentable. */
        UNROUTABLE,
        /** El broker respondió basic.nack. */
        NACK,
        /** El flujo de confirms terminó sin resultado. */
        NO_CONFIRMATION,
        /** No llegó el confirm dentro del timeout por intento. */
        TIMEOUT
    }

    private final Reason reason;

    public MessagePublishException(Reason reason, String message) {
        super(message);
        this.reason = reason;
    }

    public MessagePublishException(Reason reason, String message, Throwable cause) {
        super(message, cause);
        this.reason = reason;
    }

    public Reason getReason() {
        return reason;
    }

    /** Un mensaje no enrutable es un error de topología: reintentar no lo arregla. */
    public boolean isRetryable() {
        return reason != Reason.UNROUTABLE;
    }
}
