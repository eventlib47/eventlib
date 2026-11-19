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
package io.github.eventlib47.eventlib.broker.rabbit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import io.github.eventlib47.eventlib.broker.MessagePublishException;
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.broker.rabbit.routing.HashKeyResolver;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.OutboundMessage;
import reactor.rabbitmq.SendOptions;
import reactor.rabbitmq.Sender;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/**
 * Publicador RabbitMQ con publisher confirms y mandatory (basic.return).
 *
 * <p>Cada intento termina en uno de: ACK (éxito), NACK, no enrutable,
 * sin confirmación o timeout. La política decide si se reintenta.</p>
 */
@Slf4j
public class RabbitMQPublisher implements MessagePublisher {

    private final Sender sender;
    private final EventRoutingResolver routingResolver;
    private final HashKeyResolver hashKeyResolver;
    private final ObjectMapper objectMapper;
    private final String appName;
    private final ResiliencePolicy policy;

    public RabbitMQPublisher(
            Sender sender,
            EventRoutingResolver routingResolver,
            HashKeyResolver hashKeyResolver) {
        this(sender, routingResolver, hashKeyResolver,
                new ObjectMapper().findAndRegisterModules(),
                "unknown-service", new RabbitProperties());
    }

    /** Compatibilidad: deriva la política de las propiedades legacy del publisher. */
    public RabbitMQPublisher(
            Sender sender,
            EventRoutingResolver routingResolver,
            HashKeyResolver hashKeyResolver,
            ObjectMapper objectMapper,
            String appName,
            RabbitProperties rabbitProperties) {
        this(sender, routingResolver, hashKeyResolver, objectMapper, appName,
                legacyPolicy(rabbitProperties));
    }

    public RabbitMQPublisher(
            Sender sender,
            EventRoutingResolver routingResolver,
            HashKeyResolver hashKeyResolver,
            ObjectMapper objectMapper,
            String appName,
            ResiliencePolicy policy) {
        this.sender = Objects.requireNonNull(sender, "sender no puede ser null");
        this.routingResolver = Objects.requireNonNull(
                routingResolver, "routingResolver no puede ser null");
        this.hashKeyResolver = Objects.requireNonNull(
                hashKeyResolver, "hashKeyResolver no puede ser null");
        this.objectMapper = Objects.requireNonNull(
                objectMapper, "objectMapper no puede ser null");
        this.appName = appName == null || appName.isBlank()
                ? "unknown-service" : appName;
        this.policy = Objects.requireNonNull(policy, "policy no puede ser null");
    }

    private static ResiliencePolicy legacyPolicy(RabbitProperties props) {
        RabbitProperties safe = props == null ? new RabbitProperties() : props;
        ResilienceProperties rp = new ResilienceProperties();
        rp.getRetry().setMaxAttempts(Math.max(0, safe.getPublisherMaxRetries()) + 1);
        rp.getRetry().setBackoff(Duration.ofSeconds(2));
        rp.getRetry().setMaxBackoff(Duration.ofSeconds(2));
        rp.setTimeout(Duration.ofSeconds(Math.max(1, safe.getPublisherConfirmTimeoutSeconds())));
        rp.getCircuitBreaker().setEnabled(false);
        rp.getBulkhead().setEnabled(false);
        return new ResiliencePolicy("rabbit-publisher", rp, EventLibMetrics.noop());
    }

    @Override
    public Mono<Void> publish(String exchange, String routingKey, byte[] payload) {
        if (exchange == null || exchange.isBlank()) {
            return Mono.error(new IllegalArgumentException(
                    "El exchange no puede ser null ni estar vacío"));
        }
        if (routingKey == null || routingKey.isBlank()) {
            return Mono.error(new IllegalArgumentException(
                    "La routing key no puede ser null ni estar vacía"));
        }
        if (payload == null) {
            return Mono.error(new IllegalArgumentException(
                    "El payload no puede ser null"));
        }

        return policy.protect(() -> sendOnce(exchange, routingKey, payload))
                .onErrorMap(TimeoutException.class, error -> new MessagePublishException(
                        MessagePublishException.Reason.TIMEOUT,
                        "RabbitMQ no confirmó a tiempo. exchange=" + exchange
                                + ", routingKey=" + routingKey, error))
                .doOnSuccess(ignored -> policy.metrics().published("ack"))
                .doOnError(error -> policy.metrics().published(outcomeOf(error)));
    }

    /** Un único intento de envío con confirm. */
    private Mono<Void> sendOnce(String exchange, String routingKey, byte[] payload) {
        OutboundMessage message = new OutboundMessage(
                exchange, routingKey, messageProperties(payload), payload);
        // trackReturned activa mandatory: un mensaje sin ruta vuelve como "returned".
        SendOptions options = new SendOptions().trackReturned(true);

        return sender.sendWithPublishConfirms(Mono.just(message), options)
                .next()
                .switchIfEmpty(Mono.error(new MessagePublishException(
                        MessagePublishException.Reason.NO_CONFIRMATION,
                        "RabbitMQ no devolvió confirmación. exchange="
                                + exchange + ", routingKey=" + routingKey)))
                .flatMap(result -> {
                    if (result.isReturned()) {
                        return Mono.<Void>error(new MessagePublishException(
                                MessagePublishException.Reason.UNROUTABLE,
                                "Mensaje no enrutable: no existe una cola "
                                        + "vinculada al exchange '" + exchange
                                        + "' con routing key '" + routingKey + "'."));
                    }
                    if (!result.isAck()) {
                        return Mono.<Void>error(new MessagePublishException(
                                MessagePublishException.Reason.NACK,
                                "RabbitMQ rechazó el mensaje (nack). exchange="
                                        + exchange + ", routingKey=" + routingKey));
                    }
                    return Mono.<Void>empty();
                });
    }

    public Mono<Void> publish(IntegrationEvent event, byte[] payload) {
        Objects.requireNonNull(event, "event no puede ser null");

        EventRoute route = routingResolver.resolve(event);
        String routingKey = route.routingKey();

        if (route.exchangeType()
                == RabbitProperties.ExchangeType.X_CONSISTENT_HASH) {
            try {
                JsonNode jsonNode = objectMapper.readTree(payload);
                routingKey = hashKeyResolver.resolve(route.eventName(), jsonNode);
            } catch (Exception exception) {
                return Mono.error(new IllegalArgumentException(
                        "No se pudo resolver la clave de hash para el evento '"
                                + route.eventName() + "'.", exception));
            }
        }

        log.debug(
                "[eventlib] Publicando evento '{}' en exchange '{}' con routing key '{}'",
                route.eventName(), route.exchange(), routingKey);

        return publish(route.exchange(), routingKey, payload);
    }

    @Override
    public Mono<Void> publishRaw(String topic, String payload) {
        if (payload == null) {
            return Mono.error(new IllegalArgumentException(
                    "El payload no puede ser null"));
        }
        if (topic == null || topic.isBlank()) {
            return Mono.error(new IllegalArgumentException(
                    "El topic no puede ser null ni estar vacío"));
        }

        try {
            JsonNode jsonNode = objectMapper.readTree(payload);

            if (jsonNode == null
                    || !jsonNode.hasNonNull("eventName")
                    || jsonNode.get("eventName").asText().isBlank()) {
                return publish(
                        routingResolver.defaultExchange(),
                        topic,
                        payload.getBytes(StandardCharsets.UTF_8));
            }

            String eventName = jsonNode.get("eventName").asText();
            EventRoute route = routingResolver.resolve(eventName);
            String routingKey = route.routingKey();

            if (route.exchangeType()
                    == RabbitProperties.ExchangeType.X_CONSISTENT_HASH) {
                routingKey = hashKeyResolver.resolve(eventName, jsonNode);
            }

            return publish(
                    route.exchange(),
                    routingKey,
                    payload.getBytes(StandardCharsets.UTF_8));
        } catch (Exception exception) {
            return Mono.error(new IllegalArgumentException(
                    "No se pudo resolver la ruta del mensaje RabbitMQ.", exception));
        }
    }

    private AMQP.BasicProperties messageProperties(byte[] payload) {
        String eventId = null;
        String eventName = null;

        try {
            JsonNode json = objectMapper.readTree(payload);
            eventName = text(json, "eventName");
            eventId = text(json.path("metadata"), "eventId");
        } catch (Exception error) {
            log.debug("[eventlib] Payload sin JSON parseable; se publica como bytes.");
        }

        AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .contentType("application/json")
                .contentEncoding(StandardCharsets.UTF_8.name())
                .timestamp(new Date())
                .appId(appName);

        if (eventId != null) {
            builder.messageId(eventId).correlationId(eventId);
        }
        if (eventName != null) {
            builder.type(eventName);
        }
        return builder.build();
    }

    private static String text(JsonNode node, String field) {
        if (node == null || !node.hasNonNull(field)) {
            return null;
        }
        String value = node.get(field).asText();
        return value == null || value.isBlank() ? null : value;
    }

    private static String outcomeOf(Throwable error) {
        if (error instanceof MessagePublishException publishError) {
            return publishError.getReason().name().toLowerCase(Locale.ROOT);
        }
        return "error";
    }
}
