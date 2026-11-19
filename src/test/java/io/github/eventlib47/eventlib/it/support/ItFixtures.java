package io.github.eventlib47.eventlib.it.support;

import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.config.RabbitProperties;
import io.github.eventlib47.eventlib.config.ResilienceProperties;
import io.github.eventlib47.eventlib.core.IntegrationEvent;
import io.github.eventlib47.eventlib.producer.routing.EventRoute;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import reactor.core.publisher.Mono;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utilidades compartidas por las pruebas de integración.
 */
public final class ItFixtures {

    /** Migraciones del starter, cargadas desde el classpath de main. */
    public static final String[] OUTBOX_MIGRATIONS = {
            "db/migration/V1__create_outbox_events.sql",
            "db/migration/V2__outbox_hardening.sql"
    };

    private ItFixtures() {
    }

    /** Id de evento único para que las pruebas no compartan claves en Redis. */
    public static String uniqueEventId() {
        return "it-" + UUID.randomUUID();
    }

    /**
     * Resolver con una sola ruta TOPIC para el evento {@code test.event}.
     */
    public static EventRoutingResolver resolver(String exchange, String routingKey, String queue) {
        EventRoute route = new EventRoute(
                "it",
                "test.event",
                exchange,
                routingKey,
                queue,
                RabbitProperties.ExchangeType.TOPIC,
                1,
                false,
                null);

        return new EventRoutingResolver() {
            @Override
            public EventRoute resolve(IntegrationEvent event) {
                return route;
            }

            @Override
            public EventRoute resolve(String eventName) {
                return route;
            }

            @Override
            public Collection<EventRoute> resolveAll() {
                return List.of(route);
            }

            @Override
            public String defaultExchange() {
                return exchange;
            }
        };
    }

    /**
     * Política rápida y determinista: un solo intento, sin circuit breaker ni
     * bulkhead, para que cada fallo del broker llegue tal cual a la prueba.
     */
    public static ResilienceProperties singleAttemptResilience() {
        ResilienceProperties properties = new ResilienceProperties();
        properties.setTimeout(Duration.ofSeconds(5));
        properties.getRetry().setMaxAttempts(1);
        properties.getCircuitBreaker().setEnabled(false);
        properties.getBulkhead().setEnabled(false);
        return properties;
    }

    /**
     * Ejecuta migraciones SQL del classpath con JDBC (pgjdbc admite varias
     * sentencias en un solo {@code execute}).
     */
    public static void runMigrations(
            String jdbcUrl, String user, String password, String... classpathResources)
            throws Exception {
        try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password);
             Statement statement = connection.createStatement()) {
            for (String resource : classpathResources) {
                try (InputStream in = ItFixtures.class.getClassLoader()
                        .getResourceAsStream(resource)) {
                    if (in == null) {
                        throw new IllegalStateException(
                                "Migración no encontrada en el classpath: " + resource);
                    }
                    statement.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
                }
            }
        }
    }

    /**
     * Publisher programable: cuenta intentos, puede tardar y puede fallar.
     * {@code successes} solo sube si la publicación termina (no si se cancela).
     */
    public static final class ScriptedPublisher implements MessagePublisher {

        private final AtomicInteger calls = new AtomicInteger();
        private final AtomicInteger successes = new AtomicInteger();
        private volatile Duration delay = Duration.ZERO;
        private volatile boolean failing;

        public ScriptedPublisher delay(Duration delay) {
            this.delay = delay;
            return this;
        }

        public ScriptedPublisher failing(boolean failing) {
            this.failing = failing;
            return this;
        }

        public int calls() {
            return calls.get();
        }

        public int successes() {
            return successes.get();
        }

        @Override
        public Mono<Void> publish(String exchange, String routingKey, byte[] payload) {
            return Mono.defer(() -> {
                calls.incrementAndGet();
                if (failing) {
                    return Mono.<Void>error(new IllegalStateException("Broker caído (simulado)"));
                }
                return Mono.delay(delay)
                        .then(Mono.<Void>fromRunnable(successes::incrementAndGet));
            });
        }

        @Override
        public Mono<Void> publishRaw(String topic, String payload) {
            return Mono.error(new UnsupportedOperationException("No usado en las ITs"));
        }
    }
}
