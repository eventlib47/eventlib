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
import io.github.eventlib47.eventlib.broker.MessagePublisher;
import io.github.eventlib47.eventlib.core.MessageSerializer;
import io.github.eventlib47.eventlib.producer.DirectEventPublisher;
import io.github.eventlib47.eventlib.producer.EventPublisher;
import io.github.eventlib47.eventlib.producer.routing.EventRoutingResolver;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import reactor.core.publisher.Mono;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PublisherAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PublisherAutoConfiguration.class))
            .withBean(MessagePublisher.class, () -> mock(MessagePublisher.class))
            .withBean(MessageSerializer.class, () -> new MessageSerializer(new ObjectMapper()))
            .withBean(EventRoutingResolver.class, () -> mock(EventRoutingResolver.class));

    @Test
    void registersDirectPublisherByDefault() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(EventPublisher.class);
            assertThat(context).hasSingleBean(DirectEventPublisher.class);
        });
    }

    @Test
    void registersDirectPublisherWhenOutboxIsExplicitlyDisabled() {
        runner.withPropertyValues("eventlib.infra.outbox.enabled=false")
                .run(context -> assertThat(context).hasSingleBean(DirectEventPublisher.class));
    }

    @Test
    void doesNotRegisterDirectPublisherWhenOutboxIsEnabled() {
        runner.withPropertyValues("eventlib.infra.outbox.enabled=true")
                .run(context -> assertThat(context).doesNotHaveBean(EventPublisher.class));
    }

    @Test
    void backsOffWhenTheApplicationDefinesItsOwnPublisher() {
        runner.withBean("customPublisher", EventPublisher.class, () -> event -> Mono.empty())
                .run(context -> {
                    assertThat(context).hasSingleBean(EventPublisher.class);
                    assertThat(context).doesNotHaveBean(DirectEventPublisher.class);
                });
    }
}
