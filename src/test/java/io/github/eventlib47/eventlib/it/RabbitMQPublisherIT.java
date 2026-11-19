package io.github.eventlib47.eventlib.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.eventlib47.eventlib.broker.MessagePublishException;
import io.github.eventlib47.eventlib.broker.MessagePublishException.Reason;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitMQPublisher;
import io.github.eventlib47.eventlib.broker.rabbit.routing.DefaultHashKeyResolver;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.rabbitmq.BindingSpecification;
import reactor.rabbitmq.ExchangeSpecification;
import reactor.rabbitmq.QueueSpecification;
import reactor.rabbitmq.RabbitFlux;
import reactor.rabbitmq.Receiver;
import reactor.rabbitmq.ReceiverOptions;
import reactor.rabbitmq.Sender;
import reactor.rabbitmq.SenderOptions;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static io.github.eventlib47.eventlib.it.support.ItFixtures.resolver;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.singleAttemptResilience;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bloque 2: publisher confirms, mandatory (UNROUTABLE) y NACK contra un
 * RabbitMQ real.
 */
@Testcontainers(disabledWithoutDocker = true)
class RabbitMQPublisherIT {

    @Container
    static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management-alpine"));

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(15);

    private static Connection connection;
    private static Sender sender;
    private static Receiver receiver;

    @BeforeAll
    static void connect() throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getAmqpPort());
        factory.setUsername(RABBIT.getAdminUsername());
        factory.setPassword(RABBIT.getAdminPassword());
        connection = factory.newConnection("eventlib-publisher-it");

        Mono<Connection> connectionMono = Mono.just(connection);
        sender = RabbitFlux.createSender(new SenderOptions().connectionMono(connectionMono));
        receiver = RabbitFlux.createReceiver(
                new ReceiverOptions().connectionMono(connectionMono));
    }

    @AfterAll
    static void disconnect() throws Exception {
        if (sender != null) {
            sender.close();
        }
        if (receiver != null) {
            receiver.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    /** Declara exchange topic + cola + binding y devuelve un publisher para esa ruta. */
    private static RabbitMQPublisher declare(
            String exchange, String queue, String routingKey, Map<String, Object> queueArgs) {
        QueueSpecification queueSpec = QueueSpecification.queue(queue).durable(true);
        if (queueArgs != null) {
            queueSpec = queueSpec.arguments(queueArgs);
        }

        sender.declareExchange(ExchangeSpecification.exchange(exchange).type("topic").durable(true))
                .then(sender.declareQueue(queueSpec))
                .then(sender.bind(BindingSpecification.binding(exchange, routingKey, queue)))
                .block(VERIFY_TIMEOUT);

        ResiliencePolicy policy = new ResiliencePolicy(
                "it-publisher", singleAttemptResilience(), EventLibMetrics.noop());
        return new RabbitMQPublisher(
                sender,
                resolver(exchange, routingKey, queue),
                new DefaultHashKeyResolver(),
                new ObjectMapper(),
                "eventlib-it",
                policy);
    }

    private static byte[] body(String text) {
        return ("{\"eventName\":\"test.event\",\"text\":\"" + text + "\"}")
                .getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void routableMessage_isConfirmedAndDelivered() {
        String suffix = UUID.randomUUID().toString();
        String exchange = "it.ex." + suffix;
        String queue = "it.q." + suffix;
        RabbitMQPublisher publisher = declare(exchange, queue, "it.key", null);
        byte[] payload = body("hola");

        StepVerifier.create(publisher.publish(exchange, "it.key", payload))
                .expectComplete().verify(VERIFY_TIMEOUT);

        StepVerifier.create(receiver.consumeAutoAck(queue).next().timeout(VERIFY_TIMEOUT))
                .assertNext(delivery -> {
                    assertEquals(new String(payload, StandardCharsets.UTF_8),
                            new String(delivery.getBody(), StandardCharsets.UTF_8));
                    assertEquals(2, delivery.getProperties().getDeliveryMode(),
                            "El mensaje debe publicarse persistente");
                    assertEquals("eventlib-it", delivery.getProperties().getAppId());
                })
                .verifyComplete();
    }

    @Test
    void unroutableMessage_failsWithUnroutable() {
        String suffix = UUID.randomUUID().toString();
        String exchange = "it.ex." + suffix;
        RabbitMQPublisher publisher = declare(exchange, "it.q." + suffix, "it.key", null);

        StepVerifier.create(publisher.publish(exchange, "otra.clave.sin.binding", body("x")))
                .expectErrorSatisfies(error -> {
                    org.junit.jupiter.api.Assertions.assertInstanceOf(
                            MessagePublishException.class, error);
                    assertEquals(Reason.UNROUTABLE, ((MessagePublishException) error).getReason());
                })
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void fullQueueWithRejectPublish_failsWithNack() {
        String suffix = UUID.randomUUID().toString();
        String exchange = "it.ex." + suffix;
        String queue = "it.q." + suffix;
        RabbitMQPublisher publisher = declare(exchange, queue, "it.key",
                Map.of("x-max-length", 1, "x-overflow", "reject-publish"));

        // El primero llena la cola; el segundo es rechazado por el broker (basic.nack).
        StepVerifier.create(publisher.publish(exchange, "it.key", body("uno")))
                .expectComplete().verify(VERIFY_TIMEOUT);

        StepVerifier.create(publisher.publish(exchange, "it.key", body("dos")))
                .expectErrorSatisfies(error -> {
                    org.junit.jupiter.api.Assertions.assertInstanceOf(
                            MessagePublishException.class, error);
                    assertEquals(Reason.NACK, ((MessagePublishException) error).getReason());
                })
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void invalidArguments_failFastWithoutTouchingTheBroker() {
        String suffix = UUID.randomUUID().toString();
        RabbitMQPublisher publisher = declare(
                "it.ex." + suffix, "it.q." + suffix, "it.key", null);

        StepVerifier.create(publisher.publish(" ", "it.key", body("x")))
                .expectError(IllegalArgumentException.class).verify(VERIFY_TIMEOUT);
        StepVerifier.create(publisher.publish("it.ex." + suffix, "", body("x")))
                .expectError(IllegalArgumentException.class).verify(VERIFY_TIMEOUT);
        StepVerifier.create(publisher.publish("it.ex." + suffix, "it.key", null))
                .expectError(IllegalArgumentException.class).verify(VERIFY_TIMEOUT);
    }
}
