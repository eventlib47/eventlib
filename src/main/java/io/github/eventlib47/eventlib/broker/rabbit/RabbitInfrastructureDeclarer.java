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

import io.github.eventlib47.eventlib.config.ExchangeBindingProperties;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.CustomExchange;
import org.springframework.amqp.core.Declarable;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class RabbitInfrastructureDeclarer {

    private final EventRoutingResolver routingResolver;
    private final RabbitProperties rabbitProperties;

    public RabbitInfrastructureDeclarer(
            EventRoutingResolver routingResolver,
            RabbitProperties rabbitProperties) {
        this.routingResolver = Objects.requireNonNull(
                routingResolver, "routingResolver no puede ser null");
        this.rabbitProperties = Objects.requireNonNull(
                rabbitProperties, "rabbitProperties no puede ser null");
    }

    public List<Declarable> declarables() {
        List<Declarable> declarables = new ArrayList<>();
        Map<String, TopicExchange> topicExchangeCache = new LinkedHashMap<>();
        Map<String, CustomExchange> hashExchangeCache = new LinkedHashMap<>();

        Collection<EventRoute> routes = routingResolver.resolveAll();

        validateRouteVirtualHosts(routes);
        validateExchangeBindings();

        TopicExchange defaultExchange =
                new TopicExchange(routingResolver.defaultExchange());
        topicExchangeCache.put(defaultExchange.getName(), defaultExchange);
        declarables.add(defaultExchange);

        for (EventRoute route : routes) {
            if (!route.requiresInfrastructureDeclaration()) continue;

            if (route.exchangeType() == RabbitProperties.ExchangeType.X_CONSISTENT_HASH) {
                declareConsistentHashRoute(
                        declarables, topicExchangeCache, hashExchangeCache, route);
            } else {
                declareTopicRoute(declarables, topicExchangeCache, route);
            }
        }

        declareExchangeBindings(declarables, topicExchangeCache);
        return declarables;
    }

    private QueueBuilder durableQueue(String name) {
        QueueBuilder builder = QueueBuilder.durable(name);

        if ("quorum".equalsIgnoreCase(rabbitProperties.getQueueType())) {
            builder.quorum();
        }

        if (rabbitProperties.getMaxQueueLength() != null
                && rabbitProperties.getMaxQueueLength() > 0) {
            builder.maxLength(rabbitProperties.getMaxQueueLength());
            builder.overflow(QueueBuilder.Overflow.rejectPublish);
        }

        if (rabbitProperties.getMessageTtl() != null
                && !rabbitProperties.getMessageTtl().isZero()
                && !rabbitProperties.getMessageTtl().isNegative()) {
            builder.withArgument(
                    "x-message-ttl",
                    rabbitProperties.getMessageTtl().toMillis());
        }

        return builder;
    }

    private void declareTopicRoute(
            List<Declarable> declarables,
            Map<String, TopicExchange> exchangeCache,
            EventRoute route) {

        TopicExchange exchange = exchangeCache.computeIfAbsent(
                route.exchange(),
                exchangeName -> {
                    TopicExchange created = new TopicExchange(exchangeName);
                    declarables.add(created);
                    return created;
                }
        );

        if (!route.hasQueue()) return;

        String suffix = dlqSuffix();
        String dlqRoutingKey = route.routingKey() + suffix;

        QueueBuilder queueBuilder = durableQueue(route.queueName())
                .withArgument("x-dead-letter-exchange", route.exchange())
                .withArgument("x-dead-letter-routing-key", dlqRoutingKey);

        if (rabbitProperties.isSingleActiveConsumer()) {
            queueBuilder.withArgument("x-single-active-consumer", true);
        }

        Queue queue = queueBuilder.build();
        Queue dlq = durableQueue(route.queueName() + suffix).build();

        declarables.add(queue);
        declarables.add(
                BindingBuilder.bind(queue).to(exchange).with(route.routingKey()));

        declarables.add(dlq);
        declarables.add(
                BindingBuilder.bind(dlq).to(exchange).with(dlqRoutingKey));
    }

    private void declareConsistentHashRoute(
            List<Declarable> declarables,
            Map<String, TopicExchange> topicExchangeCache,
            Map<String, CustomExchange> hashExchangeCache,
            EventRoute route) {

        CustomExchange hashExchange = hashExchangeCache.computeIfAbsent(
                route.exchange(),
                exchangeName -> {
                    CustomExchange created = new CustomExchange(
                            exchangeName, "x-consistent-hash", true, false);
                    declarables.add(created);
                    return created;
                }
        );

        if (!route.hasQueue()) return;

        String suffix = dlqSuffix();
        String dlqRoutingKey = route.routingKey() + suffix;
        String dlqQueueName = route.queueName() + suffix;

        TopicExchange dlqExchange = topicExchangeCache.computeIfAbsent(
                routingResolver.defaultExchange(),
                exchangeName -> {
                    TopicExchange created = new TopicExchange(exchangeName);
                    declarables.add(created);
                    return created;
                }
        );

        Queue dlq = durableQueue(dlqQueueName).build();
        declarables.add(dlq);
        declarables.add(
                BindingBuilder.bind(dlq).to(dlqExchange).with(dlqRoutingKey));

        for (int shard = 0; shard < route.shardCount(); shard++) {
            String shardQueueName = route.queueName() + "-" + shard;

            Queue shardQueue = durableQueue(shardQueueName)
                    .withArgument(
                            "x-dead-letter-exchange",
                            routingResolver.defaultExchange())
                    .withArgument(
                            "x-dead-letter-routing-key", dlqRoutingKey)
                    .build();

            declarables.add(shardQueue);
            declarables.add(new Binding(
                    shardQueueName,
                    Binding.DestinationType.QUEUE,
                    hashExchange.getName(),
                    "10",
                    null
            ));
        }
    }

    private void declareExchangeBindings(
            List<Declarable> declarables,
            Map<String, TopicExchange> exchangeCache) {

        Map<String, ExchangeBindingProperties> bindings =
                rabbitProperties.getExchangeBindings();

        if (bindings == null || bindings.isEmpty()) return;

        for (Map.Entry<String, ExchangeBindingProperties> entry : bindings.entrySet()) {
            ExchangeBindingProperties config = entry.getValue();
            if (config == null || !config.isEnabled()) continue;

            String source = requireText(config.getSource(), "source", entry.getKey());
            String destination = requireText(
                    config.getDestination(), "destination", entry.getKey());
            String routingKey = requireText(
                    config.getRoutingKey(), "routingKey", entry.getKey());

            String sourceVhost = effectiveVirtualHost(config.getSourceVirtualHost());
            String destinationVhost = effectiveVirtualHost(
                    config.getDestinationVirtualHost());

            if (!Objects.equals(sourceVhost, destinationVhost)) {
                throw new IllegalStateException(
                        "No se puede crear el binding entre exchanges '"
                                + source + "' y '" + destination
                                + "': ambos deben pertenecer al mismo virtual host. "
                                + "sourceVirtualHost=" + sourceVhost
                                + ", destinationVirtualHost=" + destinationVhost);
            }

            TopicExchange sourceExchange = exchangeCache.computeIfAbsent(
                    source,
                    exchangeName -> {
                        TopicExchange created = new TopicExchange(exchangeName);
                        declarables.add(created);
                        return created;
                    }
            );

            TopicExchange destinationExchange = exchangeCache.computeIfAbsent(
                    destination,
                    exchangeName -> {
                        TopicExchange created = new TopicExchange(exchangeName);
                        declarables.add(created);
                        return created;
                    }
            );

            declarables.add(new Binding(
                    destinationExchange.getName(),
                    Binding.DestinationType.EXCHANGE,
                    sourceExchange.getName(),
                    routingKey,
                    null
            ));
        }
    }

    private void validateRouteVirtualHosts(Collection<EventRoute> routes) {
        String configuredVhost = effectiveVirtualHost(null);
        for (EventRoute route : routes) {
            String routeVhost = effectiveVirtualHost(route.virtualHost());
            if (!Objects.equals(configuredVhost, routeVhost)) {
                throw new IllegalStateException(
                        "La ruta del evento '" + route.eventName()
                                + "' pertenece al virtual host '" + routeVhost
                                + "', pero este declarador está configurado para '"
                                + configuredVhost + "'.");
            }
        }
    }

    private void validateExchangeBindings() {
        Map<String, ExchangeBindingProperties> bindings =
                rabbitProperties.getExchangeBindings();
        if (bindings == null) return;

        for (Map.Entry<String, ExchangeBindingProperties> entry : bindings.entrySet()) {
            ExchangeBindingProperties binding = entry.getValue();
            if (binding == null || !binding.isEnabled()) continue;

            String sourceVhost = effectiveVirtualHost(binding.getSourceVirtualHost());
            String destinationVhost = effectiveVirtualHost(
                    binding.getDestinationVirtualHost());

            if (!Objects.equals(sourceVhost, destinationVhost)) {
                throw new IllegalStateException(
                        "El binding '" + entry.getKey()
                                + "' intenta conectar exchanges de virtual hosts "
                                + "diferentes.");
            }

            if (!Objects.equals(sourceVhost, effectiveVirtualHost(null))) {
                throw new IllegalStateException(
                        "El binding '" + entry.getKey()
                                + "' necesita un RabbitAdmin conectado al virtual host '"
                                + sourceVhost + "'.");
            }
        }
    }

    private String effectiveVirtualHost(String override) {
        if (override != null && !override.isBlank()) return override.trim();
        String configured = rabbitProperties.getVirtualHost();
        return configured == null || configured.isBlank() ? "/" : configured.trim();
    }

    private String dlqSuffix() {
        RabbitProperties.DlqProperties dlq = rabbitProperties.getDlq();
        if (dlq == null || dlq.getSuffix() == null || dlq.getSuffix().isBlank()) {
            return ".dlq";
        }
        return dlq.getSuffix().trim();
    }

    private static String requireText(
            String value, String property, String bindingName) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "La propiedad '" + property
                            + "' es obligatoria para el binding '"
                            + bindingName + "'.");
        }
        return value.trim();
    }
}