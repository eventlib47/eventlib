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
package io.github.eventlib47.eventlib.producer;

import io.github.eventlib47.eventlib.producer.outbox.OutboxProcessor;
import io.github.eventlib47.eventlib.support.TestEvent;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OutboxEventPublisherTest {

    @Test
    void publishDelegatesToOutboxProcessor() {
        OutboxProcessor processor = mock(OutboxProcessor.class);
        TestEvent event = new TestEvent("evt-1", "user.created");
        when(processor.store(event)).thenReturn(Mono.empty());

        OutboxEventPublisher publisher = new OutboxEventPublisher(processor);

        StepVerifier.create(publisher.publish(event)).verifyComplete();
        verify(processor).store(event);
    }

    @Test
    void executeAndPublishFailsWhenTransactionalOperatorIsMissing() {
        OutboxProcessor processor = mock(OutboxProcessor.class);
        OutboxEventPublisher publisher = new OutboxEventPublisher(processor);

        StepVerifier.create(publisher.executeAndPublish(
                        Mono.just("ok"),
                        res -> new TestEvent("evt-1", "user.created")))
                .expectError(IllegalStateException.class)
                .verify();
    }

    @Test
    @SuppressWarnings("unchecked")
    void executeAndPublishWrapsBusinessWorkAndStoreInTransaction() {
        OutboxProcessor processor = mock(OutboxProcessor.class);
        TransactionalOperator txOperator = mock(TransactionalOperator.class);
        when(processor.store(any())).thenReturn(Mono.empty());
        when(txOperator.transactional(any(Mono.class)))
                .thenAnswer(invocation -> invocation.getArgument(0));

        OutboxEventPublisher publisher = new OutboxEventPublisher(processor, txOperator);

        StepVerifier.create(publisher.executeAndPublish(
                        Mono.just("user-123"),
                        id -> new TestEvent(id, "user.created")))
                .expectNext("user-123")
                .verifyComplete();

        verify(processor).store(any(TestEvent.class));
        verify(txOperator).transactional(any(Mono.class));
    }
}
