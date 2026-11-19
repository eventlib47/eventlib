package io.github.eventlib47.eventlib.it;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.it.support.ItFixtures.ScriptedPublisher;
import io.github.eventlib47.eventlib.producer.OutboxEventPublisher;
import io.github.eventlib47.eventlib.producer.outbox.OutboxEvent;
import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import io.github.eventlib47.eventlib.producer.outbox.OutboxRepository;
import io.github.eventlib47.eventlib.support.TestEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.UUID;

import static io.github.eventlib47.eventlib.it.support.ItFixtures.OUTBOX_MIGRATIONS;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.resolver;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.runMigrations;
import static io.github.eventlib47.eventlib.it.support.ItFixtures.uniqueEventId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bloque 3: Transactional Outbox contra PostgreSQL real.
 *
 * <p>El broker se sustituye por un {@link ScriptedPublisher} para controlar
 * éxitos, fallos y latencia; el broker real se cubre en RabbitMQPublisherIT y
 * OutboxToConsumerE2EIT.</p>
 */
@Testcontainers(disabledWithoutDocker = true)
class OutboxPostgresIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"));

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(20);

    private static ConnectionFactory connectionFactory;
    private static DatabaseClient databaseClient;
    private static OutboxRepository repository;
    private static TransactionalOperator transactionalOperator;

    private InfraProperties infra;
    private ScriptedPublisher publisher;

    @BeforeAll
    static void setUpDatabase() throws Exception {
        runMigrations(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(),
                OUTBOX_MIGRATIONS);

        // UNA sola instancia de ConnectionFactory: la transacción se enlaza a ella.
        connectionFactory = ConnectionFactories.get(ConnectionFactoryOptions.builder()
                .option(ConnectionFactoryOptions.DRIVER, "postgresql")
                .option(ConnectionFactoryOptions.HOST, POSTGRES.getHost())
                .option(ConnectionFactoryOptions.PORT, POSTGRES.getMappedPort(5432))
                .option(ConnectionFactoryOptions.USER, POSTGRES.getUsername())
                .option(ConnectionFactoryOptions.PASSWORD, POSTGRES.getPassword())
                .option(ConnectionFactoryOptions.DATABASE, POSTGRES.getDatabaseName())
                .build());

        databaseClient = DatabaseClient.create(connectionFactory);
        repository = new R2dbcRepositoryFactory(new R2dbcEntityTemplate(connectionFactory))
                .getRepository(OutboxRepository.class);
        transactionalOperator = TransactionalOperator.create(
                new R2dbcTransactionManager(connectionFactory));
    }

    @BeforeEach
    void setUp() {
        infra = new InfraProperties();
        infra.getOutbox().setLockTtl(Duration.ofSeconds(30));
        infra.getOutbox().setMaxAttempts(3);
        infra.getOutbox().setRetryBackoff(Duration.ofSeconds(1));
        infra.getOutbox().setMaxBackoff(Duration.ofSeconds(2));
        publisher = new ScriptedPublisher();
    }

    private OutboxProcessor processor(String owner) {
        return new OutboxProcessor(
                repository,
                publisher,
                new MessageSerializer(new ObjectMapper()),
                resolver("it.exchange", "test.event", "it.queue"),
                infra,
                databaseClient,
                owner);
    }

    // ------------------------------------------------------------------
    // Lado productor: la transacción de negocio es la del Outbox
    // ------------------------------------------------------------------

    @Test
    void store_withoutActiveTransaction_isRejected() {
        OutboxProcessor processor = processor("owner-no-tx");

        StepVerifier.create(processor.store(new TestEvent(uniqueEventId(), "test.event")))
                .expectError(IllegalStateException.class)
                .verify(VERIFY_TIMEOUT);
    }

    @Test
    void executeAndPublish_commitsBusinessResultAndEventTogether() throws Exception {
        String eventId = uniqueEventId();
        OutboxEventPublisher outboxPublisher =
                new OutboxEventPublisher(processor("owner-commit"), transactionalOperator);

        StepVerifier.create(outboxPublisher.executeAndPublish(
                        Mono.just("usuario-creado"),
                        value -> new TestEvent(eventId, "test.event")))
                .expectNext("usuario-creado")
                .verifyComplete();

        Row row = findByEventId(eventId);
        assertNotNull(row, "El evento debe quedar en el Outbox");
        assertEquals("PENDING", row.status);
        assertEquals(0, row.attempts);
    }

    @Test
    void executeAndPublish_rollsBackTheEvent_whenBusinessOperationFails() throws Exception {
        String eventId = uniqueEventId();
        OutboxEventPublisher outboxPublisher =
                new OutboxEventPublisher(processor("owner-rollback"), transactionalOperator);

        // El evento se guarda y DESPUÉS falla la operación: debe revertirse todo.
        Mono<String> business = Mono.just("ok");
        StepVerifier.create(transactionalOperator.transactional(
                        outboxPublisher.executeAndPublish(
                                business, value -> new TestEvent(eventId, "test.event"))
                                .then(Mono.<String>error(new IllegalStateException("falla de negocio")))))
                .expectError(IllegalStateException.class)
                .verify(VERIFY_TIMEOUT);

        assertEquals(null, findByEventId(eventId), "El rollback no debe dejar el evento");
    }

    // ------------------------------------------------------------------
    // Lado publicador: claim, lease, backoff, DEAD y recuperación
    // ------------------------------------------------------------------

    @Test
    void process_publishesAndMarksPublished() throws Exception {
        OutboxEvent event = insertPending(uniqueEventId());

        StepVerifier.create(processor("owner-ok").process(event))
                .expectComplete().verify(VERIFY_TIMEOUT);

        Row row = findById(event.getId());
        assertEquals("PUBLISHED", row.status);
        assertEquals(1, row.attempts);
        assertNotNull(row.publishedAt);
        assertEquals(1, publisher.successes());
    }

    @Test
    void failedPublication_goesToFailedWithBackoff_andRetriesAfterItElapses() throws Exception {
        OutboxEvent event = insertPending(uniqueEventId());
        publisher.failing(true);
        OutboxProcessor processor = processor("owner-fail");

        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);

        Row failed = findById(event.getId());
        assertEquals("FAILED", failed.status);
        assertEquals(1, failed.attempts);
        assertTrue(failed.lastError.contains("Broker caído"));
        assertTrue(failed.nextAttemptInFuture, "El backoff debe fijar next_attempt_at a futuro");
        assertEquals(1, publisher.calls());

        // Antes de vencer el backoff nadie puede reclamarla.
        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);
        assertEquals(1, publisher.calls(), "No debe reintentar antes del backoff");

        // Vence el backoff y el broker se recupera: se publica.
        execute("UPDATE outbox_events SET next_attempt_at = NOW() - INTERVAL '1 second' WHERE id = ?",
                event.getId());
        publisher.failing(false);

        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);

        Row published = findById(event.getId());
        assertEquals("PUBLISHED", published.status);
        assertEquals(2, published.attempts);
    }

    @Test
    void exhaustedAttempts_endInDead() throws Exception {
        infra.getOutbox().setMaxAttempts(2);
        OutboxEvent event = insertPending(uniqueEventId());
        publisher.failing(true);
        OutboxProcessor processor = processor("owner-dead");

        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);
        execute("UPDATE outbox_events SET next_attempt_at = NOW() - INTERVAL '1 second' WHERE id = ?",
                event.getId());
        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);

        Row dead = findById(event.getId());
        assertEquals("DEAD", dead.status);
        assertEquals(2, dead.attempts);

        // Una fila DEAD no vuelve a reclamarse.
        int callsBefore = publisher.calls();
        StepVerifier.create(processor.process(event)).expectComplete().verify(VERIFY_TIMEOUT);
        assertEquals(callsBefore, publisher.calls());
    }

    @Test
    void abandonedProcessingRow_withExpiredLease_isRecovered() throws Exception {
        OutboxEvent event = insertPending(uniqueEventId());
        // Simula un worker que murió a mitad de publicación.
        execute("UPDATE outbox_events SET status = 'PROCESSING', locked_by = 'worker-muerto', "
                + "locked_until = NOW() - INTERVAL '1 minute', attempts = 1 WHERE id = ?",
                event.getId());

        // findPending debe devolverla como candidata.
        StepVerifier.create(repository.findPending(10)
                        .filter(candidate -> candidate.getId().equals(event.getId())))
                .expectNextCount(1)
                .verifyComplete();

        StepVerifier.create(processor("owner-recovery").process(event))
                .expectComplete().verify(VERIFY_TIMEOUT);

        Row row = findById(event.getId());
        assertEquals("PUBLISHED", row.status);
        assertEquals(2, row.attempts, "El intento de recuperación suma uno");
    }

    @Test
    void liveLease_blocksOtherWorkers_soTwoConcurrentProcessesPublishOnce() throws Exception {
        OutboxEvent event = insertPending(uniqueEventId());
        publisher.delay(Duration.ofMillis(800));

        Mono<Void> workerA = processor("worker-A").process(event).subscribeOn(Schedulers.parallel());
        Mono<Void> workerB = processor("worker-B").process(event).subscribeOn(Schedulers.parallel());

        StepVerifier.create(Mono.when(workerA, workerB))
                .expectComplete().verify(VERIFY_TIMEOUT);

        assertEquals(1, publisher.calls(), "Solo un worker puede reclamar y publicar");
        assertEquals(1, publisher.successes());
        Row row = findById(event.getId());
        assertEquals("PUBLISHED", row.status);
        assertEquals(1, row.attempts);
    }

    @Test
    void heartbeat_renewsTheLease_whilePublishingTakesLongerThanTheTtl() throws Exception {
        infra.getOutbox().setLockTtl(Duration.ofMillis(1500));
        OutboxEvent event = insertPending(uniqueEventId());
        // La publicación dura 3,5 s: más del doble del lease.
        publisher.delay(Duration.ofMillis(3500));

        Mono<Void> worker = processor("worker-long").process(event);
        // A los 2,5 s (lease original vencido) otro worker intenta reclamar.
        Mono<Void> intruder = Mono.delay(Duration.ofMillis(2500))
                .then(Mono.defer(() -> processor("worker-intruder").process(event)));

        StepVerifier.create(Mono.when(worker, intruder))
                .expectComplete().verify(VERIFY_TIMEOUT);

        assertEquals(1, publisher.calls(), "El lease renovado debe impedir la segunda publicación");
        assertEquals("PUBLISHED", findById(event.getId()).status);
    }

    // ------------------------------------------------------------------
    // Utilidades JDBC (verifican el estado real de la tabla)
    // ------------------------------------------------------------------

    private OutboxEvent insertPending(String eventId) {
        OutboxEvent event = OutboxEvent.builder()
                .id(UUID.randomUUID())
                .eventId(eventId)
                .eventName("test.event")
                .topic("test.event")
                .exchange("it.exchange")
                .routingKey("test.event")
                .payload("{\"eventName\":\"test.event\"}")
                .createdAt(java.time.Instant.now())
                .className(TestEvent.class.getName())
                .status(OutboxEvent.STATUS_PENDING)
                .attempts(0)
                .build();
        return repository.save(event).block(VERIFY_TIMEOUT);
    }

    private void execute(String sql, UUID id) throws Exception {
        try (Connection connection = jdbc();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, id);
            statement.executeUpdate();
        }
    }

    private Row findById(UUID id) throws Exception {
        return query("WHERE id = ?", id);
    }

    private Row findByEventId(String eventId) throws Exception {
        return query("WHERE event_id = ?", eventId);
    }

    private Row query(String where, Object parameter) throws Exception {
        String sql = "SELECT status, attempts, last_error, published_at, "
                + "(next_attempt_at IS NOT NULL AND next_attempt_at > NOW()) AS next_in_future "
                + "FROM outbox_events " + where;
        try (Connection connection = jdbc();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, parameter);
            try (ResultSet rs = statement.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new Row(
                        rs.getString("status"),
                        rs.getInt("attempts"),
                        rs.getString("last_error"),
                        rs.getTimestamp("published_at"),
                        rs.getBoolean("next_in_future"));
            }
        }
    }

    private Connection jdbc() throws Exception {
        return DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private record Row(
            String status,
            int attempts,
            String lastError,
            java.sql.Timestamp publishedAt,
            boolean nextAttemptInFuture) {
    }
}
