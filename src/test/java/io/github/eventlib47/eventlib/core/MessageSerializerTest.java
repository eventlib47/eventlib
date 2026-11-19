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

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageSerializerTest {

    private final MessageSerializer serializer = new MessageSerializer(new ObjectMapper());

    @Test
    void preservesEventIdentityAndIsoTimestamp() {
        TestEvent event = new TestEvent("event-123", "test.event");

        String json = serializer.serialize(event);
        TestEvent restored = serializer.deserialize(json, TestEvent.class);

        assertEquals("event-123", restored.getEventId());
        assertEquals("test.event", restored.getEventName());
        assertTrue(json.contains("\"occurredAt\":\""));
        assertTrue(json.contains("\"eventId\":\"event-123\""));
    }

    @Test
    void serializesToBytesWithTheSameMapper() {
        TestEvent event = new TestEvent("event-456", "test.event");

        byte[] payload = serializer.serializev(event);
        TestEvent restored = serializer.deserialize(new String(payload), TestEvent.class);

        assertEquals("event-456", restored.getEventId());
    }
}
