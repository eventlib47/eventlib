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
package io.github.eventlib47.eventlib.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "eventlib.infra")
public class InfraProperties {

    private RabbitProperties rabbitmq = new RabbitProperties();
    private RedisProperties redis = new RedisProperties();
    private OutboxProperties outbox = new OutboxProperties();
    private ResilienceProperties resilience = new ResilienceProperties();

    public RabbitProperties getRabbitmq() { return rabbitmq; }
    public void setRabbitmq(RabbitProperties rabbitmq) { this.rabbitmq = rabbitmq; }

    public RedisProperties getRedis() { return redis; }
    public void setRedis(RedisProperties redis) { this.redis = redis; }

    public OutboxProperties getOutbox() { return outbox; }
    public void setOutbox(OutboxProperties outbox) { this.outbox = outbox; }

    public ResilienceProperties getResilience() { return resilience; }
    public void setResilience(ResilienceProperties resilience) { this.resilience = resilience; }
}
