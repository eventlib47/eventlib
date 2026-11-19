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
package io.github.eventlib47.eventlib.producer.outbox;

import io.github.eventlib47.eventlib.config.InfraProperties;
import io.github.eventlib47.eventlib.resilience.EventLibMetrics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.scheduling.annotation.Scheduled;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Polling del Outbox con lease líder en Redis (opcional) y apagado ordenado.
 *
 * <p>Al apagar: deja de aceptar ciclos nuevos, espera el ciclo en vuelo hasta
 * {@code shutdownTimeout} y, si no termina, lo cancela. Lo que quede en
 * PROCESSING se recupera por vencimiento del lease.</p>
 */
@Slf4j
public class OutboxScheduler implements SmartLifecycle {

    private final OutboxRepository repository;
    private final OutboxProcessor processor;
    private final InfraProperties infraProperties;
    private final OutboxDistributedLock lock;
    private final EventLibMetrics metrics;
    private final Duration shutdownTimeout;

    private final AtomicBoolean isProcessing = new AtomicBoolean(false);
    private final AtomicInteger consecutiveFailures = new AtomicInteger(0);
    private final AtomicReference<Disposable> currentCycle = new AtomicReference<>();
    private volatile boolean stopping = false;
    private volatile boolean running = false;

    public OutboxScheduler(
            OutboxRepository repository,
            OutboxProcessor processor,
            InfraProperties infraProperties,
            OutboxDistributedLock lock) {
        this(repository, processor, infraProperties, lock,
                EventLibMetrics.noop(), Duration.ofSeconds(20));
    }

    public OutboxScheduler(
            OutboxRepository repository,
            OutboxProcessor processor,
            InfraProperties infraProperties,
            OutboxDistributedLock lock,
            EventLibMetrics metrics,
            Duration shutdownTimeout) {
        this.repository = repository;
        this.processor = processor;
        this.infraProperties = infraProperties;
        this.lock = lock;
        this.metrics = metrics == null ? EventLibMetrics.noop() : metrics;
        this.shutdownTimeout = shutdownTimeout == null ? Duration.ofSeconds(20) : shutdownTimeout;
    }

    @Scheduled(fixedDelayString = "${eventlib.infra.outbox.polling-interval:5000}")
    public void processPendingEvents() {
        if (stopping) {
            return;
        }
        if (!isProcessing.compareAndSet(false, true)) {
            log.debug("Omitiendo ciclo de Outbox: el anterior sigue en curso.");
            return;
        }

        Mono<Boolean> acquire = lock != null ? lock.tryAcquire() : Mono.just(true);

        Mono<Void> processing = acquire
                .flatMap(acquired -> {
                    if (!Boolean.TRUE.equals(acquired)) {
                        log.debug("Otra instancia tiene el lease del Outbox.");
                        return Mono.<Void>empty();
                    }
                    return refreshPendingGauge().then(Mono.defer(this::processBatch));
                })
                // El reinicio del contador va ANTES de tragar el error (D7).
                .doOnSuccess(ignored -> consecutiveFailures.set(0))
                .doOnError(error -> {
                    int failures = consecutiveFailures.incrementAndGet();
                    log.error("Error en ciclo de Outbox (fallo #{}): {}",
                            failures, error.getMessage());
                    if (failures >= 3) {
                        log.warn("RabbitMQ/Redis/DB podrían estar caídos: {} fallos seguidos.",
                                failures);
                    }
                })
                .onErrorResume(error -> Mono.empty());

        Mono<Void> release = lock != null ? lock.release() : Mono.empty();

        Disposable cycle = processing
                .then(release)
                .doFinally(signal -> {
                    if (signal == SignalType.CANCEL && lock != null) {
                        lock.release().subscribe();
                    }
                    isProcessing.set(false);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe();
        currentCycle.set(cycle);
    }

    private Mono<Void> processBatch() {
        int batchSize = resolveBatchSize();
        return repository.findPending(batchSize)
                .concatMap(event -> stopping
                        ? Mono.<Void>empty()
                        : renewLeaseIfNeeded().then(processor.process(event)))
                .then();
    }

    private Mono<Void> refreshPendingGauge() {
        return repository.countPending()
                .doOnNext(metrics::outboxPending)
                .onErrorResume(error -> {
                    log.debug("No se pudo contar pendientes del Outbox: {}", error.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Void> renewLeaseIfNeeded() {
        if (lock == null) {
            return Mono.empty();
        }
        return lock.renew()
                .flatMap(renewed -> Boolean.TRUE.equals(renewed)
                        ? Mono.<Void>empty()
                        : Mono.<Void>error(new IllegalStateException(
                        "Se perdió el lease del Outbox durante el batch.")));
    }

    private int resolveBatchSize() {
        if (infraProperties != null
                && infraProperties.getOutbox() != null
                && infraProperties.getOutbox().getBatchSize() > 0) {
            return infraProperties.getOutbox().getBatchSize();
        }
        return 100;
    }

    // ---- SmartLifecycle: apagado ordenado ----

    @Override
    public void start() {
        stopping = false;
        running = true;
    }

    @Override
    public void stop() {
        stopping = true;
        long deadline = System.nanoTime() + shutdownTimeout.toNanos();
        while (isProcessing.get() && System.nanoTime() < deadline) {
            try {
                Thread.sleep(50);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        if (isProcessing.get()) {
            Disposable cycle = currentCycle.get();
            if (cycle != null) {
                cycle.dispose();
            }
            log.warn("[eventlib] Ciclo de Outbox cancelado tras {} de espera; "
                    + "las filas en PROCESSING se recuperarán al vencer su lease.", shutdownTimeout);
        }
        running = false;
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public int getPhase() {
        // Se detiene antes que los beans de infraestructura (Sender, conexiones).
        return Integer.MAX_VALUE - 1000;
    }
}
