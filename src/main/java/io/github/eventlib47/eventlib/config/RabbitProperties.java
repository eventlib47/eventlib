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

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class RabbitProperties {

    public enum ExchangeType {
        TOPIC,
        X_CONSISTENT_HASH
    }

    private String exchange = "eventlib.exchange";
    private Map<String, String> queues = new HashMap<>();
    private Map<String, String> bindings = new HashMap<>();
    private Map<String, String> exchanges = new HashMap<>();
    private String virtualHost;
    private Map<String, String> exchangeVirtualHosts = new HashMap<>();
    private Map<String, ExchangeBindingProperties> exchangeBindings = new HashMap<>();
    private DlqProperties dlq = new DlqProperties();
    private boolean declareInfrastructure = true;
    private Map<String, Boolean> declareInfrastructureOverrides = new HashMap<>();
    private Map<String, ExchangeType> exchangeTypes = new HashMap<>();
    private Map<String, Integer> shards = new HashMap<>();
    private boolean singleActiveConsumer = false;
    private String queueType = "classic";
    private Integer maxQueueLength;
    private Duration messageTtl;
    private Duration consumerRetryDelay = Duration.ofSeconds(2);
    private int consumerPrefetch = 1;
    private int publisherConfirmTimeoutSeconds = 10;
    private int publisherMaxRetries = 3;

    public String getExchange() { return exchange; }
    public void setExchange(String exchange) { this.exchange = exchange; }

    public Map<String, String> getQueues() { return queues; }
    public void setQueues(Map<String, String> queues) {
        this.queues = queues != null ? queues : new HashMap<>();
    }

    public Map<String, String> getBindings() { return bindings; }
    public void setBindings(Map<String, String> bindings) {
        this.bindings = bindings != null ? bindings : new HashMap<>();
    }

    public Map<String, String> getExchanges() { return exchanges; }
    public void setExchanges(Map<String, String> exchanges) {
        this.exchanges = exchanges != null ? exchanges : new HashMap<>();
    }

    public String getVirtualHost() { return virtualHost; }
    public void setVirtualHost(String virtualHost) { this.virtualHost = virtualHost; }

    public Map<String, String> getExchangeVirtualHosts() { return exchangeVirtualHosts; }
    public void setExchangeVirtualHosts(Map<String, String> exchangeVirtualHosts) {
        this.exchangeVirtualHosts = exchangeVirtualHosts != null
                ? exchangeVirtualHosts : new HashMap<>();
    }

    public Map<String, ExchangeBindingProperties> getExchangeBindings() {
        return exchangeBindings;
    }
    public void setExchangeBindings(
            Map<String, ExchangeBindingProperties> exchangeBindings) {
        this.exchangeBindings = exchangeBindings != null
                ? exchangeBindings : new HashMap<>();
    }

    public DlqProperties getDlq() { return dlq; }
    public void setDlq(DlqProperties dlq) {
        this.dlq = dlq != null ? dlq : new DlqProperties();
    }

    public boolean isDeclareInfrastructure() { return declareInfrastructure; }
    public void setDeclareInfrastructure(boolean declareInfrastructure) {
        this.declareInfrastructure = declareInfrastructure;
    }

    public Map<String, Boolean> getDeclareInfrastructureOverrides() {
        return declareInfrastructureOverrides;
    }
    public void setDeclareInfrastructureOverrides(
            Map<String, Boolean> declareInfrastructureOverrides) {
        this.declareInfrastructureOverrides = declareInfrastructureOverrides != null
                ? declareInfrastructureOverrides : new HashMap<>();
    }

    public Map<String, ExchangeType> getExchangeTypes() { return exchangeTypes; }
    public void setExchangeTypes(Map<String, ExchangeType> exchangeTypes) {
        this.exchangeTypes = exchangeTypes != null ? exchangeTypes : new HashMap<>();
    }

    public Map<String, Integer> getShards() { return shards; }
    public void setShards(Map<String, Integer> shards) {
        this.shards = shards != null ? shards : new HashMap<>();
    }

    public boolean isSingleActiveConsumer() { return singleActiveConsumer; }
    public void setSingleActiveConsumer(boolean singleActiveConsumer) {
        this.singleActiveConsumer = singleActiveConsumer;
    }

    public String getQueueType() { return queueType; }
    public void setQueueType(String queueType) { this.queueType = queueType; }

    public Integer getMaxQueueLength() { return maxQueueLength; }
    public void setMaxQueueLength(Integer maxQueueLength) {
        this.maxQueueLength = maxQueueLength;
    }

    public Duration getMessageTtl() { return messageTtl; }
    public void setMessageTtl(Duration messageTtl) { this.messageTtl = messageTtl; }

    public Duration getConsumerRetryDelay() { return consumerRetryDelay; }
    public void setConsumerRetryDelay(Duration consumerRetryDelay) {
        this.consumerRetryDelay = consumerRetryDelay;
    }

    public int getConsumerPrefetch() { return consumerPrefetch; }
    public void setConsumerPrefetch(int consumerPrefetch) {
        this.consumerPrefetch = consumerPrefetch;
    }

    public int getPublisherConfirmTimeoutSeconds() { return publisherConfirmTimeoutSeconds; }
    public void setPublisherConfirmTimeoutSeconds(int publisherConfirmTimeoutSeconds) {
        this.publisherConfirmTimeoutSeconds = publisherConfirmTimeoutSeconds;
    }

    public int getPublisherMaxRetries() { return publisherMaxRetries; }
    public void setPublisherMaxRetries(int publisherMaxRetries) {
        this.publisherMaxRetries = publisherMaxRetries;
    }

    public static class DlqProperties {
        private String suffix = ".dlq";
        public String getSuffix() { return suffix; }
        public void setSuffix(String suffix) { this.suffix = suffix; }
    }
}