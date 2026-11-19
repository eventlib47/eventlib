package io.github.eventlib47.eventlib.it;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.consumer.idempotency.IdempotencyStore.ClaimResult;
import io.github.eventlib47.eventlib.consumer.idempotency.ReactiveRedisIdempotencyStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

import static io.github.eventlib47.eventlib.it.support.ItFixtures.uniqueEventId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Bloque 1: idempotencia atómica con fencing, contra Redis real.
 */
@Testcontainers(disabledWithoutDocker = true)
class ReactiveRedisIdempotencyStoreIT {

    @Container
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine"))
                    .withExposedPorts(6379);

    private static final Duration VERIFY_TIMEOUT = Duration.ofSeconds(10);

    private static LettuceConnectionFactory connectionFactory;
    private static ReactiveStringRedisTemplate template;

    private final InfraProperties infra = new InfraProperties();
    private ReactiveRedisIdempotencyStore store;

    @BeforeAll
    static void connect() {
        connectionFactory = new LettuceConnectionFactory(
                REDIS.getHost(), REDIS.getMappedPort(6379));
        connectionFactory.afterPropertiesSet();
        template = new ReactiveStringRedisTemplate(connectionFactory);
    }

    @AfterAll
    static void disconnect() {
        if (connectionFactory != null) {
            connectionFactory.destroy();
        }
    }

    @BeforeEach
    void setUp() {
        infra.getRedis().setIdempotencyTtl(Duration.ofMinutes(5));
        store = new ReactiveRedisIdempotencyStore(template, infra);
    }

    private String key(String eventId) {
        return infra.getRedis().getIdempotencyPrefix() + ":" + eventId;
    }

    @Test
    void twentyConcurrentOwners_onlyOneClaims() {
        String eventId = uniqueEventId();

        Mono<List<ClaimResult>> results = Flux.range(0, 20)
                .flatMap(i -> store
                        .tryClaim(eventId, "owner-" + i, Duration.ofSeconds(30))
                        .subscribeOn(Schedulers.parallel()), 20)
                .collectList();

        StepVerifier.create(results)
                .assertNext(list -> {
                    assertEquals(20, list.size());
                    assertEquals(1, list.stream().filter(r -> r == ClaimResult.CLAIMED).count());
                    assertEquals(19, list.stream().filter(r -> r == ClaimResult.IN_PROGRESS).count());
                })
                .verifyComplete();
    }

    @Test
    void renewClaim_onlySucceedsForTheCurrentOwner() {
        String eventId = uniqueEventId();

        StepVerifier.create(store.tryClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();
        StepVerifier.create(store.renewClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(true).verifyComplete();
        StepVerifier.create(store.renewClaim(eventId, "B", Duration.ofSeconds(30)))
                .expectNext(false).verifyComplete();
        StepVerifier.create(store.renewClaim(uniqueEventId(), "A", Duration.ofSeconds(30)))
                .expectNext(false).verifyComplete();
    }

    @Test
    void expiredClaim_canBeTakenOver_andStaleOwnerIsFenced() {
        String eventId = uniqueEventId();

        StepVerifier.create(store.tryClaim(eventId, "A", Duration.ofMillis(300)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();

        // Tras expirar el claim de A, B lo toma.
        StepVerifier.create(Mono.delay(Duration.ofMillis(700))
                        .then(Mono.defer(() -> store.tryClaim(eventId, "B", Duration.ofSeconds(30)))))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();

        // A (dueño caducado) no puede completar, renovar ni liberar.
        StepVerifier.create(store.markCompleted(eventId, "A"))
                .expectNext(false).verifyComplete();
        StepVerifier.create(store.renewClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(false).verifyComplete();
        StepVerifier.create(store.releaseClaim(eventId, "A"))
                .expectNext(false).verifyComplete();

        // B sí completa; después cualquier entrega ve COMPLETED.
        StepVerifier.create(store.markCompleted(eventId, "B"))
                .expectNext(true).verifyComplete();
        StepVerifier.create(store.tryClaim(eventId, "C", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.COMPLETED).verifyComplete();
    }

    @Test
    void markCompleted_appliesTheIdempotencyTtl() {
        String eventId = uniqueEventId();

        StepVerifier.create(store.tryClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();
        StepVerifier.create(store.markCompleted(eventId, "A"))
                .expectNext(true).verifyComplete();

        StepVerifier.create(template.opsForValue().get(key(eventId)))
                .expectNext("COMPLETED").verifyComplete();
        StepVerifier.create(template.getExpire(key(eventId)))
                .assertNext(ttl -> assertTrue(
                        ttl.compareTo(Duration.ofMinutes(4)) > 0
                                && ttl.compareTo(Duration.ofMinutes(5)) <= 0,
                        "El TTL de COMPLETED debe ser ~5 min, fue " + ttl))
                .verifyComplete();
    }

    @Test
    void releaseClaim_onlyByOwner_andNeverDeletesCompleted() {
        String eventId = uniqueEventId();

        StepVerifier.create(store.tryClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();
        StepVerifier.create(store.releaseClaim(eventId, "B"))
                .expectNext(false).verifyComplete();
        StepVerifier.create(store.releaseClaim(eventId, "A"))
                .expectNext(true).verifyComplete();
        StepVerifier.create(store.tryClaim(eventId, "B", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();

        StepVerifier.create(store.markCompleted(eventId, "B"))
                .expectNext(true).verifyComplete();
        StepVerifier.create(store.releaseClaim(eventId, "B"))
                .expectNext(false).verifyComplete();
        StepVerifier.create(store.tryClaim(eventId, "D", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.COMPLETED).verifyComplete();
    }

    @Test
    void completedState_survivesDuplicateStorms() {
        String eventId = uniqueEventId();

        StepVerifier.create(store.tryClaim(eventId, "A", Duration.ofSeconds(30)))
                .expectNext(ClaimResult.CLAIMED).verifyComplete();
        StepVerifier.create(store.markCompleted(eventId, "A"))
                .expectNext(true).verifyComplete();

        StepVerifier.create(Flux.range(0, 10)
                        .flatMap(i -> store.tryClaim(eventId, "dup-" + i, Duration.ofSeconds(30)))
                        .distinct()
                        .collectList())
                .assertNext(list -> assertEquals(List.of(ClaimResult.COMPLETED), list))
                .expectComplete()
                .verify(VERIFY_TIMEOUT);
    }
}
