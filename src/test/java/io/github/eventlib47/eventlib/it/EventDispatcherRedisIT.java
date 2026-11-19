package io.github.eventlib47.eventlib.it;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.EventDispatcher;
import io.github.eventlib47.eventlib.consumer.IntegrationEventHandler;
import io.github.eventlib47.eventlib.consumer.TransientEventException;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore.ClaimResult;
import io.github.eventlib47.eventlib.consumer.idempotency.ReactiveRedisIdempotencyStore;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Signal;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.github.eventlib47.eventlib.it.support.ItFixtures.uniqueEventId;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Bloque 1 (end-to-end del consumidor sin broker): dispatcher + Redis real.
 */
@Testcontainers(disabledWithoutDocker = true)
class EventDispatcherRedisIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(15);

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate template;
    private static InfraProperties infra;
    private static ReactiveRedisIdempotencyStore store;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(
                REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        template = new ReactiveStringRedisTemplate(connectionFactory);
        infra = new InfraProperties();
        infra.getRedis().setIdempotencyTtl(Duration.ofMinutes(5));
        store = new ReactiveRedisIdempotencyStore(template, infra);
    }

    @AfterAll
    static void disconnect() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    private static EventDispatcher dispatcher(CountingHandler handler, Duration claimTtl) {
        return new EventDispatcher(
                List.<IntegrationEventHandler<?>>of(handler),
                store,
                "it-instance",
                claimTtl);
    }

    private static String key(String eventId) {
        return infra.getRedis().getIdempotencyPrefix() + ":" + eventId;
    }

    @Test
    void twoSimultaneousDeliveries_sameEventId_runHandlerOnce() {
        String eventId = uniqueEventId();
        CountingHandler handler = new CountingHandler(Duration.ofMillis(800));
        EventDispatcher dispatcher = dispatcher(handler, Duration.ofSeconds(30));
        TestEvent event = new TestEvent(eventId, "test.event");

        Mono<List<Signal<Void>>> both = Flux.merge(
                        dispatcher.dispatch(event).materialize(),
                        dispatcher.dispatch(event).materialize())
                .collectList();

        StepVerifier.create(both)
                .assertNext(signals -> {
                    long failures = signals.stream().filter(Signal::isOnError).count();
                    assertEquals(1, failures, "Exactamente una entrega debe fallar como IN_PROGRESS");
                    signals.stream().filter(Signal::isOnError).forEach(signal ->
                            org.junit.jupiter.api.Assertions.assertInstanceOf(
                                    TransientEventException.class, signal.getThrowable()));
                })
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, handler.started.get(), "El handler solo debe iniciar una vez");
        assertEquals(1, handler.completed.get());
    }

    @Test
    void redeliveryAfterCompletion_isSkippedAndStateIsCompleted() {
        String eventId = uniqueEventId();
        CountingHandler handler = new CountingHandler(Duration.ofMillis(50));
        EventDispatcher dispatcher = dispatcher(handler, Duration.ofSeconds(30));
        TestEvent event = new TestEvent(eventId, "test.event");

        StepVerifier.create(dispatcher.dispatch(event)).expectComplete().verify(VERIFY_TIMEOUT);
        StepVerifier.create(dispatcher.dispatch(event)).expectComplete().verify(VERIFY_TIMEOUT);

        assertEquals(1, handler.started.get(), "El duplicado no debe ejecutar el handler");
        StepVerifier.create(template.opsForValue().get(key(eventId)))
                .expectNext("COMPLETED").verifyComplete();
    }

    @Test
    void heartbeat_keepsTheClaimAliveBeyondItsTtl() {
        String eventId = uniqueEventId();
        // Claim de 900 ms (heartbeat cada 300 ms) y handler de 2,5 s.
        CountingHandler handler = new CountingHandler(Duration.ofMillis(2500));
        EventDispatcher dispatcher = dispatcher(handler, Duration.ofMillis(900));
        TestEvent event = new TestEvent(eventId, "test.event");

        // A los 1,8 s (el doble del TTL) otra entrega intenta reclamar.
        Mono<ClaimResult> probe = Mono.delay(Duration.ofMillis(1800))
                .then(Mono.defer(() -> store.tryClaim(
                        eventId, "intruder", Duration.ofSeconds(5))));

        StepVerifier.create(Mono.zip(dispatcher.dispatch(event).thenReturn("done"), probe))
                .assertNext(tuple -> {
                    assertEquals("done", tuple.getT1());
                    assertEquals(ClaimResult.IN_PROGRESS, tuple.getT2(),
                            "Sin heartbeat el claim habría expirado y el intruso lo habría tomado");
                })
                .expectComplete()
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, handler.completed.get());
    }

    @Test
    void lostClaim_cancelsTheRunningHandler() {
        String eventId = uniqueEventId();
        CountingHandler handler = new CountingHandler(Duration.ofSeconds(4));
        EventDispatcher dispatcher = dispatcher(handler, Duration.ofMillis(900));
        TestEvent event = new TestEvent(eventId, "test.event");

        // Simula pérdida del claim (p. ej. failover de Redis) a los 500 ms.
        Mono.delay(Duration.ofMillis(500))
                .then(template.delete(key(eventId)))
                .subscribe();

        StepVerifier.create(dispatcher.dispatch(event))
                .expectError(TransientEventException.class)
                .verify(VERIFY_TIMEOUT);

        assertEquals(1, handler.started.get());
        assertEquals(0, handler.completed.get(), "El handler debe cancelarse al perder el claim");
    }

    @Test
    void handlerFailure_releasesTheClaim_soARetryCanSucceed() {
        String eventId = uniqueEventId();
        AtomicBoolean failFirst = new AtomicBoolean(true);
        CountingHandler handler = new CountingHandler(Duration.ofMillis(20)) {
            @Override
            public Mono<Void> handle(TestEvent event) {
                if (failFirst.getAndSet(false)) {
                    started.incrementAndGet();
                    return Mono.error(new IllegalStateException("fallo simulado"));
                }
                return super.handle(event);
            }
        };
        EventDispatcher dispatcher = dispatcher(handler, Duration.ofSeconds(30));
        TestEvent event = new TestEvent(eventId, "test.event");

        StepVerifier.create(dispatcher.dispatch(event))
                .expectError(IllegalStateException.class)
                .verify(VERIFY_TIMEOUT);

        // La clave debe haberse liberado: el reintento procesa y completa.
        StepVerifier.create(dispatcher.dispatch(event)).expectComplete().verify(VERIFY_TIMEOUT);

        assertEquals(2, handler.started.get());
        assertEquals(1, handler.completed.get());
        StepVerifier.create(template.opsForValue().get(key(eventId)))
                .expectNext("COMPLETED").verifyComplete();
    }

    /** Handler que simula trabajo con un retardo y cuenta inicios y finales. */
    private static class CountingHandler implements IntegrationEventHandler<TestEvent> {

        final AtomicInteger started = new AtomicInteger();
        final AtomicInteger completed = new AtomicInteger();
        private final Duration work;

        CountingHandler(Duration work) {
            this.work = work;
        }

        @Override
        public Mono<Void> handle(TestEvent event) {
            return Mono.defer(() -> {
                started.incrementAndGet();
                return Mono.delay(work).then();
            }).doOnSuccess(ignored -> completed.incrementAndGet());
        }

        @Override
        public String getEventName() {
            return "test.event";
        }
    }
}
