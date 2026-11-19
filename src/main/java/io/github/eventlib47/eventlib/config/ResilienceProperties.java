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

/**
 * eventlib.infra.resilience.* — política única de resiliencia.
 */
public class ResilienceProperties {

    /** Timeout POR INTENTO. */
    private Duration timeout = Duration.ofSeconds(10);
    private RetryProperties retry = new RetryProperties();
    private CircuitBreakerProperties circuitBreaker = new CircuitBreakerProperties();
    private BulkheadProperties bulkhead = new BulkheadProperties();
    /** Espera entre intentos de reconexión al broker. */
    private Duration reconnectDelay = Duration.ofSeconds(5);
    /** Tiempo máximo para drenar trabajo en vuelo al apagar. */
    private Duration shutdownTimeout = Duration.ofSeconds(20);

    public Duration getTimeout() { return timeout; }
    public void setTimeout(Duration timeout) { this.timeout = timeout; }

    public RetryProperties getRetry() { return retry; }
    public void setRetry(RetryProperties retry) { this.retry = retry; }

    public CircuitBreakerProperties getCircuitBreaker() { return circuitBreaker; }
    public void setCircuitBreaker(CircuitBreakerProperties circuitBreaker) {
        this.circuitBreaker = circuitBreaker;
    }

    public BulkheadProperties getBulkhead() { return bulkhead; }
    public void setBulkhead(BulkheadProperties bulkhead) { this.bulkhead = bulkhead; }

    public Duration getReconnectDelay() { return reconnectDelay; }
    public void setReconnectDelay(Duration reconnectDelay) { this.reconnectDelay = reconnectDelay; }

    public Duration getShutdownTimeout() { return shutdownTimeout; }
    public void setShutdownTimeout(Duration shutdownTimeout) { this.shutdownTimeout = shutdownTimeout; }

    public static class RetryProperties {
        /** Intentos totales (1 = sin reintentos). */
        private int maxAttempts = 3;
        private Duration backoff = Duration.ofMillis(500);
        private Duration maxBackoff = Duration.ofSeconds(10);
        /** Factor de jitter 0..1. */
        private double jitter = 0.2;

        public int getMaxAttempts() { return maxAttempts; }
        public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
        public Duration getBackoff() { return backoff; }
        public void setBackoff(Duration backoff) { this.backoff = backoff; }
        public Duration getMaxBackoff() { return maxBackoff; }
        public void setMaxBackoff(Duration maxBackoff) { this.maxBackoff = maxBackoff; }
        public double getJitter() { return jitter; }
        public void setJitter(double jitter) { this.jitter = jitter; }
    }

    public static class CircuitBreakerProperties {
        private boolean enabled = true;
        private float failureRateThreshold = 50f;
        private int slidingWindowSize = 20;
        private int minimumNumberOfCalls = 10;
        private Duration waitDurationInOpenState = Duration.ofSeconds(10);
        private int permittedCallsInHalfOpenState = 3;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public float getFailureRateThreshold() { return failureRateThreshold; }
        public void setFailureRateThreshold(float v) { this.failureRateThreshold = v; }
        public int getSlidingWindowSize() { return slidingWindowSize; }
        public void setSlidingWindowSize(int v) { this.slidingWindowSize = v; }
        public int getMinimumNumberOfCalls() { return minimumNumberOfCalls; }
        public void setMinimumNumberOfCalls(int v) { this.minimumNumberOfCalls = v; }
        public Duration getWaitDurationInOpenState() { return waitDurationInOpenState; }
        public void setWaitDurationInOpenState(Duration v) { this.waitDurationInOpenState = v; }
        public int getPermittedCallsInHalfOpenState() { return permittedCallsInHalfOpenState; }
        public void setPermittedCallsInHalfOpenState(int v) { this.permittedCallsInHalfOpenState = v; }
    }

    public static class BulkheadProperties {
        private boolean enabled = true;
        private int maxConcurrentCalls = 25;
        private Duration maxWaitDuration = Duration.ofMillis(100);

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public int getMaxConcurrentCalls() { return maxConcurrentCalls; }
        public void setMaxConcurrentCalls(int v) { this.maxConcurrentCalls = v; }
        public Duration getMaxWaitDuration() { return maxWaitDuration; }
        public void setMaxWaitDuration(Duration v) { this.maxWaitDuration = v; }
    }
}
