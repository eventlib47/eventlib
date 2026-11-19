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
package io.github.eventlib47.eventlib.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitConnectionProvider;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitInfrastructureDeclarer;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitMQPublisher;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitSslSupport;
import io.github.eventlib47.eventlib.broker.rabbit.routing.DefaultEventRoutingResolver;
import io.github.eventlib47.eventlib.broker.rabbit.routing.DefaultHashKeyResolver;
import io.github.eventlib47.eventlib.broker.rabbit.routing.HashKeyResolver;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Declarables;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ResourceLoader;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;

@Slf4j
@AutoConfiguration(before = org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration.class)
@ConditionalOnClass({RabbitTemplate.class, RabbitFlux.class})
public class RabbitAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(name = "eventLibConnectionFactory")
    public ConnectionFactory eventLibConnectionFactory(
            RabbitProperties springProps,
            ResourceLoader resourceLoader) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(springProps.getHost());
        factory.setPort(springProps.getPort());
        factory.setUsername(springProps.getUsername());
        factory.setPassword(springProps.getPassword());
        if (springProps.getVirtualHost() != null) {
            factory.setVirtualHost(springProps.getVirtualHost());
        }
        if (springProps.getConnectionTimeout() != null) {
            factory.setConnectionTimeout(
                    Math.toIntExact(springProps.getConnectionTimeout().toMillis()));
        }
        if (springProps.getRequestedHeartbeat() != null) {
            factory.setRequestedHeartbeat(
                    Math.toIntExact(springProps.getRequestedHeartbeat().toSeconds()));
        }

        RabbitSslSupport.configure(factory, springProps.getSsl(), resourceLoader);

        // La recuperación la gestiona RabbitConnectionProvider (reactiva).
        factory.setAutomaticRecoveryEnabled(false);
        factory.setExceptionHandler(new com.rabbitmq.client.impl.DefaultExceptionHandler() {
            @Override
            public void handleUnexpectedConnectionDriverException(
                    Connection conn, Throwable exception) {
                log.debug("[{}] Excepción de driver AMQP absorbida por resiliencia reactiva: {}",
                        conn.getClientProvidedName(), exception.getMessage());
            }
        });
        return factory;
    }

    @Bean
    public RabbitConnectionProvider rabbitConnectionProvider(
            @Qualifier("eventLibConnectionFactory") ConnectionFactory factory,
            InfraProperties infraProperties,
            EventLibMetrics metrics,
            @Value("${spring.application.name:unknown-service}") String appName) {
        return new RabbitConnectionProvider(
                factory, appName, infraProperties.getResilience(), metrics);
    }

    @Bean
    public Mono<? extends Connection> producerConnectionMono(
            RabbitConnectionProvider provider) {
        return provider.producerConnection();
    }

    @Bean
    public Mono<? extends Connection> consumerConnectionMono(
            RabbitConnectionProvider provider) {
        return provider.consumerConnection();
    }

    @Bean
    @ConditionalOnMissingBean
    public Sender rabbitSender(
            @Qualifier("producerConnectionMono") Mono<? extends Connection> connectionMono) {
        return RabbitFlux.createSender(
                new SenderOptions().connectionMono(connectionMono));
    }

    @Bean
    @ConditionalOnMissingBean
    public Receiver rabbitReceiver(
            @Qualifier("consumerConnectionMono") Mono<? extends Connection> connectionMono) {
        return RabbitFlux.createReceiver(
                new ReceiverOptions().connectionMono(connectionMono));
    }

    @Bean
    @ConditionalOnMissingBean(name = "springAmqpConnectionFactory")
    public CachingConnectionFactory springAmqpConnectionFactory(
            @Qualifier("eventLibConnectionFactory") ConnectionFactory connectionFactory,
            RabbitProperties springProps) {
        CachingConnectionFactory factory = new CachingConnectionFactory(connectionFactory);
        if (springProps.getPublisherConfirmType() != null) {
            factory.setPublisherConfirmType(springProps.getPublisherConfirmType());
        } else {
            factory.setPublisherConfirmType(CachingConnectionFactory.ConfirmType.CORRELATED);
        }
        factory.setPublisherReturns(true);
        return factory;
    }

    @Bean
    @ConditionalOnMissingBean(RabbitTemplate.class)
    public RabbitTemplate rabbitTemplate(
            @Qualifier("springAmqpConnectionFactory") CachingConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMandatory(true);
        return template;
    }

    @Bean
    @ConditionalOnMissingBean
    public RabbitAdmin rabbitAdmin(
            @Qualifier("springAmqpConnectionFactory") CachingConnectionFactory connectionFactory) {
        RabbitAdmin admin = new RabbitAdmin(connectionFactory);
        admin.setAutoStartup(true);
        return admin;
    }

    @Bean
    @ConditionalOnMissingBean
    public EventRoutingResolver eventRoutingResolver(InfraProperties infraProperties) {
        return new DefaultEventRoutingResolver(infraProperties);
    }

    @Bean
    @ConditionalOnMissingBean
    public RabbitInfrastructureDeclarer rabbitInfrastructureDeclarer(
            EventRoutingResolver routingResolver,
            InfraProperties infraProperties
    ) {
        return new RabbitInfrastructureDeclarer(
                routingResolver,
                infraProperties.getRabbitmq()
        );
    }

    @Bean
    public Declarables rabbitEntities(RabbitInfrastructureDeclarer declarer) {
        return new Declarables(declarer.declarables());
    }

    @Bean
    @ConditionalOnMissingBean
    public HashKeyResolver hashKeyResolver() {
        return new DefaultHashKeyResolver();
    }

    /** Política central del publicador: configurable en eventlib.infra.resilience.*. */
    @Bean
    @ConditionalOnMissingBean(name = "rabbitPublisherResiliencePolicy")
    public ResiliencePolicy rabbitPublisherResiliencePolicy(
            InfraProperties infraProperties,
            EventLibMetrics metrics) {
        return new ResiliencePolicy(
                "rabbit-publisher", infraProperties.getResilience(), metrics);
    }

    @Bean
    @ConditionalOnMissingBean(MessagePublisher.class)
    public MessagePublisher messagePublisher(
            Sender sender,
            EventRoutingResolver routingResolver,
            HashKeyResolver hashKeyResolver,
            ObjectProvider<ObjectMapper> objectMapperProvider,
            @Qualifier("rabbitPublisherResiliencePolicy") ResiliencePolicy policy,
            @Value("${spring.application.name:unknown-service}") String appName
    ) {
        ObjectMapper objectMapper = objectMapperProvider.getIfAvailable(
                () -> new ObjectMapper().findAndRegisterModules());
        return new RabbitMQPublisher(
                sender,
                routingResolver,
                hashKeyResolver,
                objectMapper,
                appName,
                policy
        );
    }

    @Bean
    public org.springframework.boot.ApplicationRunner initializeInfrastructure(
            @Qualifier("rabbitAdmin") RabbitAdmin rabbitAdmin,
            Declarables rabbitEntities) {
        return args -> {
            try {
                rabbitAdmin.initialize();
                log.info("[eventlib] Infraestructura RabbitMQ declarada.");
            } catch (Exception error) {
                throw new IllegalStateException(
                        "No se pudo declarar la infraestructura RabbitMQ; "
                                + "el servicio no debe arrancar en modo degradado.",
                        error);
            }
        };
    }
}
