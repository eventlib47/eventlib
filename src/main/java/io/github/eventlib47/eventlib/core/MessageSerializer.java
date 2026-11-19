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
/**
 * Serializador JSON genérico.
 */



package io.github.eventlib47.eventlib.core;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public class MessageSerializer {

    private final ObjectMapper mapper;

    public MessageSerializer(ObjectMapper mapper) {
        this.mapper = mapper.copy()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    public String serialize(Object object) {
        try {
            return this.mapper.writeValueAsString(object);
        } catch (Exception error) {
            throw new IllegalArgumentException(
                    "Error serializing to String: " + error.getMessage(), error);
        }
    }

    public byte[] serializev(Object event) {
        try {
            return this.mapper.writeValueAsBytes(event);
        } catch (Exception error) {
            throw new IllegalArgumentException(
                    "Error serializing to bytes: " + error.getMessage(), error);
        }
    }

    public <T> T deserialize(String payload, Class<T> type) {
        try {
            return this.mapper.readValue(payload, type);
        } catch (Exception error) {
            throw new IllegalArgumentException(
                    "Error deserializing: " + error.getMessage(), error);
        }
    }
}
