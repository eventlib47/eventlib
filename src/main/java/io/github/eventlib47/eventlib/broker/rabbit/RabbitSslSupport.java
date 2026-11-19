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
package io.github.eventlib47.eventlib.broker.rabbit;

import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.amqp.RabbitProperties;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.Collection;

/**
 * Traduce `spring.rabbitmq.ssl.*` a un {@link SSLContext} real.
 *
 * <p>Sin trust store, Java usa el trust store por defecto de la JVM. La
 * validación de cadena nunca se desactiva; `validate-server-certificate=false`
 * solo registra una advertencia y sigue validando.</p>
 */
@Slf4j
public final class RabbitSslSupport {

    private RabbitSslSupport() {
    }

    public static void configure(
            ConnectionFactory factory,
            RabbitProperties.Ssl ssl,
            ResourceLoader resourceLoader) {

        if (ssl == null || !ssl.determineEnabled()) {
            return;
        }

        /*if (!ssl.isValidateServerCertificate()) {
            log.warn("[eventlib] validate-server-certificate=false no desactiva "
                    + "la validación de cadena; configure un trust store válido.");
        }*/
        if (ssl.isVerifyHostname()) {
            factory.enableHostnameVerification();
        }

        try {
            SSLContext context = buildContext(ssl, resourceLoader);
            factory.useSslProtocol(context);
            //if (ssl.getVerifyHostname()) {
            if (ssl.isVerifyHostname()) {
                factory.enableHostnameVerification();
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "No se pudo configurar TLS de RabbitMQ. Revise "
                            + "spring.rabbitmq.ssl.trust-store/key-store.", error);
        }
    }

    private static SSLContext buildContext(
            RabbitProperties.Ssl ssl,
            ResourceLoader resourceLoader) throws Exception {

        KeyStore trustStore = loadStore(
                ssl.getTrustStore(),
                ssl.getTrustStoreType(),
                ssl.getTrustStorePassword(),
                resourceLoader,
                true);

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(
                valueOrDefault(ssl.getTrustStoreAlgorithm(),
                        TrustManagerFactory.getDefaultAlgorithm()));
        trustManagerFactory.init(trustStore);

        KeyManagerFactory keyManagerFactory = null;
        if (hasText(ssl.getKeyStore())) {
            KeyStore keyStore = loadStore(
                    ssl.getKeyStore(),
                    ssl.getKeyStoreType(),
                    ssl.getKeyStorePassword(),
                    resourceLoader,
                    false);
            keyManagerFactory = KeyManagerFactory.getInstance(
                    valueOrDefault(ssl.getKeyStoreAlgorithm(),
                            KeyManagerFactory.getDefaultAlgorithm()));
            keyManagerFactory.init(keyStore, password(ssl.getKeyStorePassword()));
        }

        SSLContext context = SSLContext.getInstance(
                valueOrDefault(ssl.getAlgorithm(), "TLSv1.2"));
        context.init(
                keyManagerFactory == null ? null : keyManagerFactory.getKeyManagers(),
                trustManagerFactory.getTrustManagers(),
                null);
        return context;
    }

    private static KeyStore loadStore(
            String location,
            String type,
            String storePassword,
            ResourceLoader resourceLoader,
            boolean trustStore) throws Exception {

        if (!hasText(location)) {
            return null;
        }

        Resource resource = resourceLoader.getResource(location);
        if (!resource.exists()) {
            throw new IllegalArgumentException(
                    "No existe el archivo de certificados: " + location);
        }

        String effectiveType = valueOrDefault(type, KeyStore.getDefaultType());
        boolean pem = location.toLowerCase().endsWith(".pem")
                || location.toLowerCase().endsWith(".crt");

        if (pem) {
            if (!trustStore) {
                throw new IllegalArgumentException(
                        "El key store PEM no es soportado; use PKCS12/JKS para "
                                + "spring.rabbitmq.ssl.key-store.");
            }
            return loadPemTrustStore(resource);
        }

        KeyStore store = KeyStore.getInstance(effectiveType);
        try (InputStream input = resource.getInputStream()) {
            store.load(input, password(storePassword));
        }
        return store;
    }

    private static KeyStore loadPemTrustStore(Resource resource) throws Exception {
        KeyStore store = KeyStore.getInstance("PKCS12");
        store.load(null, null);

        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        try (InputStream input = resource.getInputStream()) {
            Collection<? extends Certificate> certificates =
                    certificateFactory.generateCertificates(input);
            int index = 0;
            for (Certificate certificate : certificates) {
                store.setCertificateEntry("eventlib-ca-" + index++, certificate);
            }
            if (index == 0) {
                throw new IllegalArgumentException(
                        "El trust store PEM no contiene certificados: "
                                + resource.getDescription());
            }
        }
        return store;
    }

    private static char[] password(String value) {
        return value == null ? null : value.toCharArray();
    }

    private static boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private static String valueOrDefault(String value, String fallback) {
        return hasText(value) ? value : fallback;
    }
}
