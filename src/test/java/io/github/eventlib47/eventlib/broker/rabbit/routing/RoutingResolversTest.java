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
package io.github.eventlib47.eventlib.broker.rabbit.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RoutingResolversTest {

    @Test
    void resolvesDefaultAndCustomEventRoutes() {
        InfraProperties infra = new InfraProperties();
        RabbitProperties rabbit = infra.getRabbitmq();
        rabbit.setExchange("global.exchange");
        rabbit.setVirtualHost("/prod");
        rabbit.setExchanges(Map.of("payment.paid", "payments.exchange"));
        rabbit.setBindings(Map.of("payment.paid", "payment.paid.v1"));
        rabbit.setQueues(Map.of("payment.paid", "payments.queue"));
        rabbit.setExchangeTypes(Map.of("payment.paid", RabbitProperties.ExchangeType.X_CONSISTENT_HASH));
        rabbit.setShards(Map.of("payment.paid", 8));
        rabbit.setDeclareInfrastructureOverrides(Map.of("payment.paid", false));
        rabbit.setExchangeVirtualHosts(Map.of("payment.paid", "/payments"));

        DefaultEventRoutingResolver resolver = new DefaultEventRoutingResolver(infra);

        assertEquals("global.exchange", resolver.defaultExchange());
        assertEquals(1, resolver.resolveAll().size());

        EventRoute custom = resolver.resolve(new TestEvent("1", "payment.paid"));
        assertEquals("payments.exchange", custom.exchange());
        assertEquals("payment.paid.v1", custom.routingKey());
        assertEquals("payments.queue", custom.queueName());
        assertEquals(RabbitProperties.ExchangeType.X_CONSISTENT_HASH, custom.exchangeType());
        assertEquals(8, custom.shardCount());
        assertFalse(custom.declareInfrastructure());
        assertEquals("/payments", custom.virtualHost());
        assertTrue(custom.hasQueue());

        EventRoute fallback = resolver.resolve("user.deleted");
        assertEquals("global.exchange", fallback.exchange());
        assertEquals("user.deleted", fallback.routingKey());
        assertEquals(RabbitProperties.ExchangeType.TOPIC, fallback.exchangeType());
        assertEquals(4, fallback.shardCount());
        assertTrue(fallback.declareInfrastructure());
        assertEquals("/prod", fallback.virtualHost());

        assertThrows(IllegalArgumentException.class, () -> resolver.resolve(" "));
    }

    @Test
    void hashKeyResolverPrefersAggregateIdAndFallsBackToEventId() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        DefaultHashKeyResolver resolver = new DefaultHashKeyResolver();

        var withAggregate = mapper.readTree("{\"metadata\":{\"eventId\":\"e-1\",\"aggregateId\":\"agg-9\"}}");
        var withoutAggregate = mapper.readTree("{\"metadata\":{\"eventId\":\"e-1\"}}");

        assertEquals("agg-9", resolver.resolve("order.created", withAggregate));
        assertEquals("e-1", resolver.resolve("order.created", withoutAggregate));
    }
}
