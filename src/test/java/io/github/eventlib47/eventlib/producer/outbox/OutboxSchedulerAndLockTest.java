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
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.data.redis.core.script.RedisScript;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxSchedulerAndLockTest {

    @Test
    @SuppressWarnings("unchecked")
    void distributedLockAcquiresRenewsAndReleases() {
        ReactiveStringRedisTemplate redis = mock(ReactiveStringRedisTemplate.class);
        ReactiveValueOperations<String, String> valueOps = mock(ReactiveValueOperations.class);
        when(redis.opsForValue()).thenReturn(valueOps);
        when(valueOps.setIfAbsent(eq("eventlib:outbox:leader-lock:billing"), anyString(), eq(Duration.ofSeconds(60))))
                .thenReturn(Mono.just(true));
        when(redis.execute(any(RedisScript.class), eq(List.of("eventlib:outbox:leader-lock:billing")), any(List.class)))
                .thenReturn(Flux.just(1L))
                .thenReturn(Flux.just(1L));

        InfraProperties infra = new InfraProperties();
        infra.getOutbox().setLockTtl(Duration.ZERO); // cae al default de 60s
        OutboxDistributedLock lock = new OutboxDistributedLock(redis, infra, "billing");

        StepVerifier.create(lock.tryAcquire()).expectNext(true).verifyComplete();
        StepVerifier.create(lock.renew()).expectNext(true).verifyComplete();
        StepVerifier.create(lock.release()).verifyComplete();
    }

    @Test
    void schedulerSkipsBatchWhenLockIsHeldByAnotherInstance() {
        OutboxRepository repository = mock(OutboxRepository.class);
        OutboxProcessor processor = mock(OutboxProcessor.class);
        OutboxDistributedLock lock = mock(OutboxDistributedLock.class);
        when(lock.tryAcquire()).thenReturn(Mono.just(false));
        when(lock.release()).thenReturn(Mono.empty());

        OutboxScheduler scheduler = new OutboxScheduler(
                repository, processor, new InfraProperties(), lock);
        scheduler.start();
        assertTrue(scheduler.isRunning());

        scheduler.processPendingEvents();

        verify(lock, timeout(1000)).tryAcquire();
        verify(lock, timeout(1000)).release();
        verify(repository, never()).findPending(anyInt());

        scheduler.stop();
        assertFalse(scheduler.isRunning());
    }

    @Test
    void schedulerProcessesBatchWhenLeaderLockIsAcquired() {
        OutboxRepository repository = mock(OutboxRepository.class);
        OutboxProcessor processor = mock(OutboxProcessor.class);
        OutboxDistributedLock lock = mock(OutboxDistributedLock.class);

        OutboxEvent event = OutboxEvent.builder().id(UUID.randomUUID()).eventName("test.event").build();
        when(lock.tryAcquire()).thenReturn(Mono.just(true));
        when(lock.renew()).thenReturn(Mono.just(true));
        when(lock.release()).thenReturn(Mono.empty());
        when(repository.countPending()).thenReturn(Mono.just(1L));
        when(repository.findPending(10)).thenReturn(Flux.just(event));
        when(processor.process(event)).thenReturn(Mono.empty());

        InfraProperties infra = new InfraProperties();
        infra.getOutbox().setBatchSize(10);

        OutboxScheduler scheduler = new OutboxScheduler(
                repository, processor, infra, lock, EventLibMetrics.noop(), Duration.ofSeconds(2));
        scheduler.processPendingEvents();

        verify(processor, timeout(1000)).process(event);
        verify(lock, timeout(1000)).release();
    }
}
