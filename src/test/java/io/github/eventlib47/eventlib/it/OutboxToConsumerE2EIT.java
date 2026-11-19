package io.github.eventlib47.eventlib.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitMQPublisher;
import io.github.eventlib47.eventlib.broker.rabbit.RabbitMessageReceiver;
import io.github.eventlib47.eventlib.broker.rabbit.routing.DefaultHashKeyResolver;
import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.IntegrationEventHandler;
import io.github.eventlib47.eventlib.consumer.idempotency.ReactiveRedisIdempotencyStore;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.OutboxEventPublisher;
import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import io.github.eventlib47.eventlib.producer.outbox.OutboxRepository;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import io.github.eventlib47.eventlib.resilience.ResiliencePolicy;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import io.github.eventlib47.eventlib.support.TestEvent;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.Disposable;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.eventlib47.eventlib.it.support.ItFixtures.OUTBOX_MIGRATIONS;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.resolver;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.runMigrations;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.singleAttemptResilience;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.uniqueEventId;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * E2E real: negocio (tx R2DBC) → Outbox → RabbitMQ (confirms) → consumidor
 * con idempotencia en Redis → ACK.
 */
@Testcontainers(disabledWithoutDocker = true)
class OutboxToConsumerE2EIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);

    @Container
    static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:3.13-management-alpine"));

    private static final String EXCHANGE = "e2e.exchange";
    private static final String ROUTING_KEY = "test.event";
    private static final String QUEUE = "e2e.queue";
    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(20);

    private static Connection producerConnection;
    private static Connection consumerConnection;
    private static Connection adminConnection;
    private static Sender sender;
    private static Receiver receiver;
    private static LettuceConnectionFactory redisFactory;
    private static ReactiveStringRedisTemplate redisTemplate;
    private static InfraProperties infra;
    private static OutboxRepository repository;
    private static OutboxProcessor outboxProcessor;
    private static OutboxEventPublisher outboxPublisher;
    private static RabbitMQPublisher rabbitPublisher;
    private static MessageSerializer serializer;
    private static final AtomicInteger HANDLED = new AtomicInteger();

    @BeforeAll
    static void bootstrap() throws Exception {
        // --- PostgreSQL / R2DBC ---
        runMigrations(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                OUTBOX_MIGRATIONS);
        io.r2dbc.spi.ConnectionFactory r2dbc = ConnectionFactories.get(
                ConnectionFactoryOptions.builder()
                        .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                        .option(ConnectionFactoryOptions.HOST, POSTGRES.getHost())
                        .option(ConnectionFactoryOptions.PORT, POSTGRES.getMappedPort(5432))
                        .option(ConnectionFactoryOptions.USER, POSTGRES.getUsername())
                        .option(ConnectionFactoryOptions.PASSWORD, POSTGRES.getPassword())
                        .option(ConnectionFactoryOptions.DATABASE, POSTGRES.getDatabaseName())
                        .build());
        DatabaseClient databaseClient = DatabaseClient.create(r2dbc);
        repository = new R2dbcRepositoryFactory(new R2dbcEntityTemplate(r2dbc))
                .getRepository(OutboxRepository.class);
        TransactionalOperator tx = TransactionalOperator.create(new R2dbcTransactionManager(r2dbc));

        // --- Redis ---
        redisFactory = new LettuceConnectionFactory(REDIS.getHost(), REDIS.getMappedPort(6379));
        redisFactory.afterPropertiesSet();
        redisTemplate = new ReactiveStringRedisTemplate(redisFactory);
        infra = new InfraProperties();
        infra.getRedis().setIdempotencyTtl(Duration.ofMinutes(5));
        ReactiveRedisIdempotencyStore store = new ReactiveRedisIdempotencyStore(redisTemplate, infra);

        // --- RabbitMQ: tres conexiones (productor, consumidor, administración) ---
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getAmqpPort());
        factory.setUsername(RABBIT.getAdminUsername());
        factory.setPassword(RABBIT.getAdminPassword());
        producerConnection = factory.newConnection("e2e-producer");
        consumerConnection = factory.newConnection("e2e-consumer");
        adminConnection = factory.newConnection("e2e-admin");

        sender = RabbitFlux.createSender(
                new SenderOptions().connectionMono(Mono.just(producerConnection)));
        receiver = RabbitFlux.createReceiver(
                new ReceiverOptions().connectionMono(Mono.just(consumerConnection)));

        sender.declareExchange(ExchangeSpecification.exchange(EXCHANGE).type("topic").durable(true))
                .then(sender.declareQueue(QueueSpecification.queue(QUEUE).durable(true)))
                .then(sender.bind(BindingSpecification.binding(EXCHANGE, ROUTING_KEY, QUEUE)))
                .block(VERIFY_TIMEOUT);

        // --- Componentes de eventlib ---
        EventRoutingResolver resolver = resolver(EXCHANGE, ROUTING_KEY, QUEUE);
        serializer = new MessageSerializer(new ObjectMapper());
        rabbitPublisher = new RabbitMQPublisher(
                sender, resolver, new DefaultHashKeyResolver(), new ObjectMapper(), "e2e",
                new ResiliencePolicy("e2e-publisher", singleAttemptResilience(),
                        EventLibMetrics.noop()));

        outboxProcessor = new OutboxProcessor(
                repository, rabbitPublisher, serializer, resolver, infra, databaseClient,
                "e2e-outbox-owner");
        outboxPublisher = new OutboxEventPublisher(outboxProcessor, tx);

        // --- Consumidor ---
        IntegrationEventHandler<TestEvent> handler = new IntegrationEventHandler<>() {
            @Override
            public Mono<Void> handle(TestEvent event) {
                return Mono.fromRunnable(HANDLED::incrementAndGet);
            }

            @Override
            public String getEventName() {
                return "test.event";
            }
        };
        EventDispatcher dispatcher = new EventDispatcher(
                List.<IntegrationEventHandler<?>>of(handler), store,
                "e2e-consumer", Duration.ofSeconds(30));
        RabbitMessageReceiver messageReceiver = new RabbitMessageReceiver(
                receiver, serializer, dispatcher, Duration.ofMillis(200), 1);

        consumerSubscription = messageReceiver.consume(QUEUE, TestEvent.class).subscribe();
    }

    private static Disposable consumerSubscription;

    @AfterAll
    static void tearDown() throws Exception {
        if (consumerSubscription != null) {
            consumerSubscription.dispose();
        }
        if (sender != null) {
            sender.close();
        }
        if (receiver != null) {
            receiver.close();
        }
        for (Connection connection : new Connection[]{
                producerConnection, consumerConnection, adminConnection}) {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        }
        if (redisFactory != null) {
            redisFactory.destroy();
        }
    }

    @Test
    void businessTransaction_outbox_broker_consumer_andDuplicateIsIgnored() throws Exception {
        String eventId = uniqueEventId();
        int before = HANDLED.get();

        // 1) Negocio + evento en UNA transacción.
        StepVerifier.create(outboxPublisher.executeAndPublish(
                        Mono.just("usuario-" + UUID.randomUUID()),
                        value -> new TestEvent(eventId, "test.event")))
                .expectNextCount(1)
                .verifyComplete();

        // 2) El scheduler real haría esto en cada ciclo: tomar candidatas y procesarlas.
        StepVerifier.create(repository.findPending(10)
                        .filter(row -> eventId.equals(row.getEventId()))
                        .concatMap(outboxProcessor::process)
                        .then())
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        // 3) El consumidor ejecuta el handler exactamente una vez.
        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertEquals(before + 1, HANDLED.get()));

        String key = infra.getRedis().getIdempotencyPrefix() + ":" + eventId;
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertEquals("COMPLETED", redisTemplate.opsForValue().get(key).block(VERIFY_TIMEOUT)));

        // 4) Entrega duplicada del mismo evento (p. ej. republicación del Outbox).
        String payload = serializer.serialize(new TestEvent(eventId, "test.event"));
        StepVerifier.create(rabbitPublisher.publish(
                        EXCHANGE, ROUTING_KEY, payload.getBytes(StandardCharsets.UTF_8)))
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        // Damos margen al consumidor para procesar (y descartar) el duplicado.
        Thread.sleep(1500);
        assertEquals(before + 1, HANDLED.get(), "El duplicado no debe ejecutar el handler");

        // 5) Todo quedó confirmado (ACK): al cerrar al consumidor no regresa nada a la cola.
        consumerSubscription.dispose();
        consumerConnection.close();
        try (Channel channel = adminConnection.createChannel()) {
            assertNull(channel.basicGet(QUEUE, true),
                    "No debe quedar ningún mensaje sin ACK ni pendiente");
        }
    }
}
