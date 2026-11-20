# EventLib Spring Boot Starter

Starter reactivo. Librería para integración de eventos en arquitecturas de microservicios con Spring Boot. EventLib abstrae la publicación y consumo de eventos sobre RabbitMQ, y añade mecanismos de idempotencia atómica con Redis, un Transactional Outbox opcional sobre R2DBC y una política común de resiliencia con Reactor y Resilience4j.

| Dato | Valor |
|---|---|
| Versión | `1.0.1` |
| Java | 17+ (compilado con `java.version=17`) |
| Spring Boot | 3.5.15 |
| Licencia | Apache License 2.0 |
| Broker soportado | RabbitMQ (AMQP 0-9-1) |
| Persistencia del Outbox | PostgreSQL + R2DBC |
| Coordenadas | `io.github.eventlib47:eventlib-spring-boot-starter:1.0.1` |

> Este documento describe la versión `1.0.1`. EventLib es un starter de Spring Boot orientado a sistemas distribuidos y arquitecturas event-driven. Su objetivo es reducir el código de infraestructura que cada microservicio necesita para publicar, recibir y procesar eventos de integración de forma resiliente y (**desacoplar**) arquitecturas (monolíticas) que tengan problemas de **Escalabilidad**.

## Badges

![Java](https://img.shields.io/badge/Java-17%2B-orange)
![Spring Boot](https://img.shields.io/badge/Spring%20Boot-3.5.15-brightgreen)
![License](https://img.shields.io/badge/License-Apache%202.0-blue)
![Status](https://img.shields.io/badge/Status-Release%201.0.1-success)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-Ready-orange)
![Reactive](https://img.shields.io/badge/Reactive-Reactor-blueviolet)

## Contenido

1. [Descripción](#1-descripción)
2. [Inicio rápido](#2-inicio-rápido)
3. [Características principales](#3-características-principales)
4. [Arquitectura y flujo](#4-arquitectura-y-flujo)
5. [Pila tecnológica](#5-pila-tecnológica)
6. [Requisitos previos](#6-requisitos-previos)
7. [Estructura del proyecto](#7-estructura-del-proyecto)
8. [Configuración](#8-configuración)
9. [Uso: productor](#9-uso-productor)
10. [Uso: consumidor](#10-uso-consumidor)
11. [Idempotencia](#11-idempotencia)
12. [Transactional Outbox](#12-transactional-outbox)
13. [RabbitMQ, TLS y topología](#13-rabbitmq-tls-y-topología)
14. [Observabilidad](#14-observabilidad)
15. [Build, pruebas, calidad y release](#15-build-pruebas-calidad-y-release)
16. [Migración desde 1.0.0](#16-migración-desde-100)
17. [Solución de problemas](#17-solución-de-problemas)
18. [Seguridad y licencia](#18-seguridad-y-licencia)

## 1. Descripción

EventLib se integra como starter de Spring Boot y aporta:

- **Productor:** `EventPublisher` para publicar `IntegrationEvent` por `eventName`, con implementación directa (`DirectEventPublisher`) o transaccional mediante Outbox (`OutboxEventPublisher`).
- **Consumidor:** `IntegrationEventHandler<T>` por `eventName` y un `EventDispatcher` que aplica idempotencia antes de ejecutar el handler.
- **Transporte:** `MessagePublisher`/`MessageReceiver` como puertos agnósticos y su implementación RabbitMQ (`RabbitMQPublisher`, `RabbitMessageReceiver`).
- **Infraestructura:** declaración automática de exchanges, colas, bindings y DLQ, con arranque *fail-fast* si la topología no se puede declarar.
- **Resiliencia:** timeout por intento, reintentos con backoff, circuit breaker, bulkhead y reconexión reactiva.
- **Observabilidad:** métricas de Micrometer y un `ReactiveHealthIndicator` opcional cuando Actuator está presente.

El diseño es reactivo (Project Reactor). No incluye un ORM ni un driver R2DBC: el servicio aporta el driver de su base de datos. Tampoco incluye una implementación Kafka; los puertos `MessagePublisher`/`MessageReceiver` existen para permitirla en el futuro.

## 2. Inicio rápido
### 2.0 Infraestructura local con Docker

```yaml
services:
  # PostgreSQL Database (Outbox)
  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_DB: ${POSTGRES_DB:-eventlib}
      POSTGRES_USER: ${POSTGRES_USER:-eventlib_user}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:?Definir POSTGRES_PASSWORD en .env}
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init-db.sql:/docker-entrypoint-initdb.d/init-db.sql:ro
    networks:
      - eventlib-network
    restart: always
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-eventlib_user} -d ${POSTGRES_DB:-eventlib}"]
      interval: 10s
      timeout: 5s
      retries: 5

  # RabbitMQ Message Broker
  rabbitmq:
    image: rabbitmq:3.12-management-alpine
    environment:
      RABBITMQ_DEFAULT_USER: ${RABBITMQ_USER:-guest}
      RABBITMQ_DEFAULT_PASS: ${RABBITMQ_PASS:?Definir RABBITMQ_PASS en .env}
      RABBITMQ_DEFAULT_VHOST: ${RABBITMQ_VHOST:-/}
    ports:
      - "${RABBITMQ_AMQP_PORT:-5672}:5672"       # AMQP estándar
      - "${RABBITMQ_TLS_PORT:-5671}:5671"        # TLS/SSL
      - "${RABBITMQ_MGMT_PORT:-15672}:15672"    # Management UI
    volumes:
      - rabbitmq_data:/var/lib/rabbitmq
      - ./certs/server_certificate.pem:/etc/rabbitmq/certs/server_certificate.pem:ro
      - ./certs/server_key.pem:/etc/rabbitmq/certs/server_key.pem:ro
      - ./certs/ca_certificate.pem:/etc/rabbitmq/certs/ca_certificate.pem:ro
      - ./rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro
    networks:
      - eventlib-network
    restart: unless-stopped

  # Redis para Idempotencia / Fencing (Opcional)
  redis:
    image: redis:7-alpine
    ports:
      - "${REDIS_PORT:-6379}:6379"
    volumes:
      - redis_data:/data
    networks:
      - eventlib-network
    # restart: unless-stopped
    command: >
      redis-server
      --appendonly yes
      --maxmemory ${REDIS_MAXMEMORY:-256mb}
      --maxmemory-policy ${REDIS_MAXMEMORY_POLICY:-allkeys-lru}
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  postgres_data:
    driver: local
  rabbitmq_data:
    driver: local
  redis_data:
    driver: local
  # prometheus_data:
  #   driver: local
  # grafana_data:
  #   driver: local

networks:
  eventlib-network:
    driver: bridge
```

```bash
docker compose up -d
```

`postgres` monta `./init-db.sql` — ahí va el script de `outbox_events` (ver [Patrón Transactional Outbox](#patrón-transactional-outbox)).

**`[DISEÑADO]`** Si vas a probar el modo `X_CONSISTENT_HASH`, el broker necesita el plugin extra habilitado:

```bash
docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
```










### 2.1 Dependencia

```xml
<dependency>
    <groupId>io.github.eventlib47</groupId>
    <artifactId>eventlib-spring-boot-starter</artifactId>
    <version>1.0.1</version>
</dependency>
```

El starter incorpora `spring-boot-starter-amqp`, `spring-boot-starter-data-redis-reactive`, `spring-boot-starter-data-r2dbc`, `reactor-rabbitmq`, Jackson y Resilience4j. El driver R2DBC (por ejemplo `r2dbc-postgresql`) y el servidor de base de datos los aporta cada servicio.

### 2.2 Infraestructura mínima

- **RabbitMQ** accesible desde el arranque: EventLib declara la topología al iniciar y falla si no puede hacerlo.
- **Redis** obligatorio si se registran handlers (idempotencia). En pruebas puede habilitarse el store nulo con `eventlib.infra.redis.allow-no-op=true`.
- **PostgreSQL + R2DBC** solo si se activa el Transactional Outbox.

### 2.3 Definir un evento

Todo evento extiende `IntegrationEvent` y devuelve un nombre estable. Ese nombre debe coincidir con el del handler y con el usado en la configuración de routing.

```java
import io.github.eventlib47.eventlib.core.IntegrationEvent;

public class UserRegisteredEvent extends IntegrationEvent {

    private String userId;
    private String email;

    public UserRegisteredEvent() {
        super();
    }

    public UserRegisteredEvent(String userId, String email) {
        super();
        this.userId = userId;
        this.email = email;
    }

    @Override
    public String getEventName() {
        return "auth.user.v1.created";
    }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEmail() { return email; }
    public void setEmail(String email) { this.email = email; }
}
```

`IntegrationEvent` crea automáticamente `metadata.eventId` (UUID) y `metadata.occurredAt`. El constructor sin argumentos es necesario para la deserialización con Jackson. Para exchanges `x-consistent-hash`, llama a `setAggregateId(...)` desde el constructor si necesitas conservar el orden por agregado; si no se define, se usa `eventId` como clave.

### 2.4 Productor

Publicación directa (Outbox desactivado):


```java
import io.github.eventlib47.eventlib.producer.EventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
public class RegistrationService {

    private final EventPublisher eventPublisher;

    public RegistrationService(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    public Mono<Void> register(User user) {
        return userRepository.save(user)
                .then(eventPublisher.publish(new UserRegisteredEvent(user.getId(), user.getEmail())));
    }
}
```

Publicación transaccional (Outbox activado):


> **Nota:** `@EnableR2dbcRepositories` en el `main` del productor,
Apuntar al paquete R2DBC **de tu propio microservicio**. El Outbox de `eventlib` funciona si tu repositorios es R2DBC, no necesitas esta anotación si `eventlib.infra.outbox.enabled=false no nesecitas esta notacion:
`@EnableR2dbcRepositories(basePackageClasses = OutboxRepository.class)`.



```java
package com.example.auth_services;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;

@SpringBootApplication
@EnableR2dbcRepositories(basePackages = "com.example.auth_services.infrastructure.repository.r2dbc")
public class AuthServicesApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuthServicesApplication.class, args);
    }
}
```
Productor con **Transactional** en su **logica de negocio** (Outbox activado):

```java
import io.github.eventlib47.eventlib.producer.OutboxEventPublisher;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@org.springframework.transaction.annotation.Transactional
public class RegistrationService {

    private final OutboxEventPublisher outboxPublisher;

    public RegistrationService(OutboxEventPublisher outboxPublisher) {
        this.outboxPublisher = outboxPublisher;
    }

    public Mono<User> register(User user) {
        return outboxPublisher.executeAndPublish(
                userRepository.save(user),
                saved -> new UserRegisteredEvent(saved.getId(), saved.getEmail()));
    }
}
```

Con `eventlib.infra.outbox.enabled=true`, el bean `EventPublisher` es `OutboxEventPublisher`. Si llamas directamente a `publish(...)`, debes hacerlo dentro de una transacción R2DBC (por defecto `require-transaction=true`). `executeAndPublish(...)` encapsula operación de negocio y evento en una sola transacción, Garantiza que, si el usuario no se guarda en la base de datos, el evento no se envíe al broker.

### 2.5 Consumidor

```java
import io.github.eventlib47.eventlib.consumer.IntegrationEventHandler;
import io.github.eventlib47.eventlib.consumer.PermanentEventException;
import io.github.eventlib47.eventlib.consumer.TransientEventException;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class UserRegisteredHandler implements IntegrationEventHandler<UserRegisteredEvent> {

    @Override
    public String getEventName() {
        return "auth.user.v1.created";
    }

    @Override
    public Mono<Void> handle(UserRegisteredEvent event) {
        return sendWelcomeEmail(event)
                .onErrorMap(InvalidRecipientException.class,
                        error -> new PermanentEventException("Destinatario inválido", error))
                .onErrorMap(MailProviderUnavailableException.class,
                        error -> new TransientEventException("Proveedor no disponible", error));
    }
}
```

El starter no se suscribe automáticamente a ninguna cola. El servicio debe iniciar el consumo:

```java
import io.github.eventlib47.eventlib.broker.MessageReceiver;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class EventSubscriptionConfig {

    @Bean
    public ApplicationRunner subscribe(MessageReceiver receiver) {
        return args -> receiver
                .consume("email.user.v1.created.queue", UserRegisteredEvent.class)
                .subscribe();
    }
}
```

### 2.6 Configuración mínima

Productor con Outbox y TLS:

```properties
spring.application.name=auth-services

spring.r2dbc.url=r2dbc:postgresql://localhost:5433/ms_auth_services
spring.r2dbc.username=${DB_USER}
spring.r2dbc.password=${DB_PASSWORD}

spring.rabbitmq.host=localhost
spring.rabbitmq.port=5671
spring.rabbitmq.username=${RABBIT_USER}
spring.rabbitmq.password=${RABBIT_PASSWORD}
spring.rabbitmq.ssl.enabled=true
spring.rabbitmq.ssl.verify-hostname=true
spring.rabbitmq.ssl.trust-store=file:./certs/ca_certificate.pem

eventlib.infra.rabbitmq.exchange=auth.exchange
eventlib.infra.rabbitmq.declare-infrastructure=true
eventlib.infra.rabbitmq.exchanges.auth_user_created=auth.exchange
eventlib.infra.rabbitmq.bindings.auth_user_created=auth.user.v1.created

eventlib.infra.outbox.enabled=true
eventlib.infra.outbox.batch-size=50
eventlib.infra.outbox.polling-interval=5000
eventlib.infra.outbox.lock-enabled=true
eventlib.infra.outbox.lock-ttl=60s
eventlib.infra.outbox.max-attempts=5
eventlib.infra.outbox.retry-backoff=3s
eventlib.infra.outbox.max-backoff=5m
eventlib.infra.outbox.require-transaction=true
```

Consumidor:

```properties
spring.application.name=email-service

spring.rabbitmq.host=localhost
spring.rabbitmq.port=5671
spring.rabbitmq.username=${RABBIT_USER}
spring.rabbitmq.password=${RABBIT_PASSWORD}
spring.rabbitmq.ssl.enabled=true
spring.rabbitmq.ssl.verify-hostname=true
spring.rabbitmq.ssl.trust-store=file:./certs/ca_certificate.pem

spring.data.redis.host=localhost
spring.data.redis.port=6379

eventlib.infra.rabbitmq.exchange=auth.exchange
eventlib.infra.rabbitmq.exchanges.auth_user_created=auth.exchange
eventlib.infra.rabbitmq.bindings.auth_user_created=auth.user.v1.created
eventlib.infra.rabbitmq.queues.auth_user_created=email.user.v1.created.queue
eventlib.infra.rabbitmq.declare-infrastructure-overrides.auth_user_created=true
eventlib.infra.rabbitmq.consumer-prefetch=1
eventlib.infra.rabbitmq.consumer-retry-delay=2s

eventlib.infra.redis.idempotency-prefix=eventlib:idempotency:email-service
eventlib.infra.redis.idempotency-ttl=24h
eventlib.infra.redis.claim-ttl=30s
eventlib.infra.redis.allow-no-op=false
```

> **Alias de topología:** las claves de `queues`, `bindings`, `exchanges`, `exchange-types`, `shards`, `declare-infrastructure-overrides` y `exchange-virtual-hosts` son alias usados al declarar infraestructura. La publicación resuelve la ruta por `getEventName()`. Si el alias no coincide con el nombre del evento, la publicación usa `exchange` global y `routingKey = getEventName()`; la declaración sí usa el alias. Mantén la topología coherente en ambos lados o usa el `eventName` como alias.

### 2.7 Compilar y probar

```bash
mvn -f eventlib/pom.xml test
mvn -f eventlib/pom.xml clean install -DskipTests -Dgpg.skip=true
mvn -q -pl auth-services,email-service -am -DskipTests compile
```

La suite unitaria actual tiene **53 pruebas** y cubre dispatcher/idempotencia, serialización, decisiones ACK/NACK, outbox, publisher y declaración de infraestructura. No hay pruebas `*IT` versionadas todavía; las dependencias de Testcontainers están disponibles para incorporarlas.

## 3. Características principales

- **Publicación por `eventName`:** resolución de exchange y routing key con valores globales y por evento.
- **Publisher confirms y mandatory:** cada envío termina en ACK, NACK, mensaje no enrutable, ausencia de confirmación o timeout. Un mensaje no enrutable es un error de topología y no se reintenta.
- **Mensajes persistentes:** `deliveryMode=2`, `content-type=application/json`, codificación UTF-8, `messageId` y `correlationId` desde `metadata.eventId`, `type=eventName` y `appId=spring.application.name`.
- **Consumo con ACK manual:** `prefetch` configurable y procesamiento secuencial por entrega (`concatMap`).
- **Idempotencia fail-closed:** claim atómico, owner con fencing, renovación por heartbeat y estados `CLAIMED`/`IN_PROGRESS`/`COMPLETED`.
- **Clasificación explícita de errores:** `PermanentEventException` va a DLQ; `TransientEventException` y errores no clasificados se reencolan; errores de deserialización van a DLQ.
- **Transactional Outbox:** persistencia en la transacción de negocio, claim atómico con `locked_by`/`locked_until`, lease renovable, backoff exponencial con jitter, estados `PENDING`/`PROCESSING`/`PUBLISHED`/`FAILED`/`DEAD` y publicación condicionada al owner.
- **Declaración de topología fail-fast:** exchanges, colas durables, bindings, DLQ, colas quorum, límites de longitud y TTL de mensajes.
- **Exchange consistente-hash:** sharding por `aggregateId` con DLQ asociada.
- **Resiliencia centralizada:** timeout por intento, reintentos con backoff, circuit breaker, bulkhead y reconexión reactiva.
- **Auto-configuración:** beans condicionales y sustituibles por el servicio.
- **Observabilidad:** métricas de publicación, outbox, reintentos, reconexiones y circuit breaker; health indicator con ping a Redis.

## 4. Arquitectura y flujo

### 4.1 Productor directo

```text
IntegrationEvent
  → DirectEventPublisher
      → EventRoutingResolver (eventName → exchange, routingKey)
      → MessageSerializer (JSON)
      → RabbitMQPublisher
          → ResiliencePolicy (timeout, retry, bulkhead, circuit breaker)
          → Sender.sendWithPublishConfirms (mandatory)
              → RabbitMQ
```

`MessagePublishException` tipifica el fallo (`UNROUTABLE`, `NACK`, `NO_CONFIRMATION`, `TIMEOUT`). Solo los fallos transitorios se reintentan.

### 4.2 Productor con Outbox

```text
Transacción R2DBC del servicio
  ├── operación de negocio
  └── OutboxEventPublisher / OutboxProcessor.store
          → INSERT en outbox_events (PENDING)

OutboxScheduler (polling)
  → lease líder en Redis (opcional)
  → OutboxRepository.findPending
  → OutboxProcessor.process
      → claim atómico (UPDATE ... RETURNING) con token único
      → RabbitMQPublisher.publish
      → renewal del lease mientras publica
      → PUBLISHED (solo si el token sigue siendo owner)
      → FAILED / DEAD con backoff
```

Garantía: **at-least-once**. Un duplicado es posible si se pierde el lease tras publicar; el consumidor lo absorbe con idempotencia.

### 4.3 Consumidor

```text
RabbitMessageReceiver.consume(queue, type)
  → deserialización JSON
  → EventDispatcher.dispatch(event)
      1. tryClaim(eventId, owner, claimTtl)
      2. COMPLETED → no ejecuta handler (ACK)
         IN_PROGRESS → TransientEventException (requeue)
         CLAIMED → busca handler por eventName
      3. heartbeat renueva el claim cada claimTtl/3
      4. handler.handle(event)
      5. markCompleted(eventId, owner) condicionado al owner
  → ACK / NACK según resultado
```

### 4.4 Matriz de ACK/NACK

| Situación | Resultado | Efecto |
|---|---|---|
| Handler completado y `markCompleted` correcto | ACK | Evento consumido |
| Duplicado con estado `COMPLETED` | ACK | Handler no se ejecuta |
| `PermanentEventException` | NACK sin requeue | Va a la DLQ |
| Handler inexistente para el `eventName` | NACK sin requeue | Va a la DLQ |
| `TransientEventException` | NACK con requeue | Reintento tras `consumer-retry-delay` |
| Claim `IN_PROGRESS` (otra entrega) | NACK con requeue | Reintento |
| Error de deserialización | NACK sin requeue | Va a la DLQ |
| Error no clasificado (DB, red, etc.) | NACK con requeue | Reintento |
| Fallo de Redis al reclamar/completar | NACK con requeue | Nunca ACK ante fallo de idempotencia |

## 5. Pila tecnológica

| Componente | Versión / referencia |
|---|---|
| Java | 17+ (servicios de referencia: Java 21) |
| Spring Boot | 3.5.3 (`spring-boot-dependencies` BOM) |
| Reactor | Project Reactor |
| RabbitMQ | `spring-boot-starter-amqp` + `reactor-rabbitmq` 1.5.6 |
| Redis | `spring-boot-starter-data-redis-reactive` (Lettuce) |
| Persistencia Outbox | `spring-boot-starter-data-r2dbc` + `DatabaseClient` |
| Serialización | Jackson (`spring-boot-starter-json`) |
| Resiliencia | Resilience4j 2.1.0 (`reactor`, `circuitbreaker`, `bulkhead`) |
| Métricas | Micrometer (`micrometer-core`) |
| Utilidades | Lombok 1.18.30 (`provided`) |
| Build | Compiler 3.11.0, Surefire/Failsafe 3.2.5, JaCoCo 0.8.12, CycloneDX 2.8.0, Enforcer 3.4.1 |
| Release | Source 3.3.0, Javadoc 3.6.3, perfil `release`: GPG 3.2.4 + `central-publishing-maven-plugin` 0.7.0 |
| Tests | JUnit 5, Mockito, `reactor-test` 3.6.4, Testcontainers 1.19.7 (RabbitMQ/PostgreSQL) |

## 6. Requisitos previos

### Obligatorios

- JDK 17 o superior.
- Maven 3.8.6 o superior (el POM lo exige con Enforcer).
- RabbitMQ accesible al arrancar.
- Una aplicación Spring Boot reactiva con un `ObjectMapper` disponible (Spring Boot lo aporta con Jackson).

### Condicionales

- **Redis:** obligatorio si hay handlers registrados. La fuente principal es `spring.data.redis.*`; `eventlib.infra.redis.host/port/password/ssl-enabled` es un fallback.
- **PostgreSQL + R2DBC:** obligatorio con `eventlib.infra.outbox.enabled=true`. El esquema debe incluir las columnas de claim del Outbox.
- **TLS de RabbitMQ:** trust store si el broker usa un certificado no confiado por la JVM. Key store PKCS12/JKS si se requiere certificado de cliente (mTLS).
- **Actuator:** opcional; habilita el health indicator.

### Versiones verificadas en este repositorio

- `eventlib 1.0.1`: `mvn -f eventlib/pom.xml test` con 28 pruebas aprobadas.
- Servicios de referencia: Java 21, Spring Boot 3.5.3, PostgreSQL, RabbitMQ con TLS en 5671 y Redis.

## 7. Estructura del proyecto

```text
eventlib/
├── pom.xml
├── README.md                  # guía corta vigente
├── README4.md                 # versión previa de la guía extendida
├── README5.md                 # este documento
├── CHANGELOG.md
├── SECURITY.md
├── LICENSE
├── NOTICE
├── DOCUMENTACION_TECNICA.md
├── MANUAL.md
├── README (3).md
├── src/
│   ├── main/
│   │   ├── java/io/github/eventlib47/eventlib/
│   │   │   ├── autoconfigure/
│   │   │   │   ├── EventLibAutoConfiguration.java
│   │   │   │   ├── RabbitAutoConfiguration.java
│   │   │   │   ├── ConsumerAutoConfiguration.java
│   │   │   │   ├── RedisAutoConfiguration.java
│   │   │   │   ├── OutboxAutoConfiguration.java
│   │   │   │   └── PublisherAutoConfiguration.java
│   │   │   ├── broker/
│   │   │   │   ├── MessagePublisher.java
│   │   │   │   ├── MessageReceiver.java
│   │   │   │   ├── MessagePublishException.java
│   │   │   │   └── rabbit/
│   │   │   │       ├── RabbitConnectionProvider.java
│   │   │   │       ├── RabbitInfrastructureDeclarer.java
│   │   │   │       ├── RabbitMQPublisher.java
│   │   │   │       ├── RabbitMessageReceiver.java
│   │   │   │       ├── RabbitSslSupport.java
│   │   │   │       └── routing/
│   │   │   │           ├── DefaultEventRoutingResolver.java
│   │   │   │           ├── DefaultHashKeyResolver.java
│   │   │   │           └── HashKeyResolver.java
│   │   │   ├── config/
│   │   │   │   ├── InfraProperties.java
│   │   │   │   ├── RabbitProperties.java
│   │   │   │   ├── RedisProperties.java
│   │   │   │   ├── OutboxProperties.java
│   │   │   │   ├── ResilienceProperties.java
│   │   │   │   └── ExchangeBindingProperties.java
│   │   │   ├── consumer/
│   │   │   │   ├── EventDispatcher.java
│   │   │   │   ├── IntegrationEventHandler.java
│   │   │   │   ├── EventProcessingException.java
│   │   │   │   ├── PermanentEventException.java
│   │   │   │   ├── TransientEventException.java
│   │   │   │   └── idempotency/
│   │   │   │       ├── IdempotencyStore.java
│   │   │   │       └── ReactiveRedisIdempotencyStore.java
│   │   │   ├── core/
│   │   │   │   ├── IntegrationEvent.java
│   │   │   │   ├── EventMetadata.java
│   │   │   │   ├── MessageSerializer.java
│   │   │   │   └── PublishEvent.java
│   │   │   ├── health/
│   │   │   │   └── EventLibHealthIndicator.java
│   │   │   ├── producer/
│   │   │   │   ├── EventPublisher.java
│   │   │   │   ├── DirectEventPublisher.java
│   │   │   │   ├── OutboxEventPublisher.java
│   │   │   │   ├── outbox/
│   │   │   │   │   ├── OutboxEvent.java
│   │   │   │   │   ├── OutboxRepository.java
│   │   │   │   │   ├── OutboxProcessor.java
│   │   │   │   │   ├── OutboxScheduler.java
│   │   │   │   │   └── OutboxDistributedLock.java
│   │   │   │   └── routing/
│   │   │   │       ├── EventRoute.java
│   │   │   │       └── EventRoutingResolver.java
│   │   │   └── resilience/
│   │   │       ├── ResiliencePolicy.java
│   │   │       └── EventLibMetrics.java
│   │   └── resources/
│   │       ├── META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports
│   │       ├── META-INF/eventlib-defaults.yml   # referencia; los defaults reales están en código
│   │       └── db/migration/
│   │           ├── V1__create_outbox_events.sql
│   │           └── V2__outbox_hardening.sql
│   └── test/java/io/github/eventlib47/eventlib/
│       ├── autoconfigure/PublisherAutoConfigurationTest.java
│       ├── broker/rabbit/ (4 tests de RabbitMQ)
│       ├── consumer/ (dispatcher y concurrencia)
│       ├── core/MessageSerializerTest.java
│       ├── producer/outbox/OutboxProcessorTest.java
│       └── support/ (RecordingIdempotencyStore, TestEvent)
└── target/                    # generado; no versionar
```

El workflow de CI vive en la raíz del repositorio (`../contextoInicial/.github/workflows/ci.yml`), no dentro de `eventlib/`.

## 8. Configuración

### 8.1 Prefijos y precedencia

- `eventlib.infra.rabbitmq.*`: topología, consumo y publicación.
- `eventlib.infra.redis.*`: idempotencia y lock del Outbox, con fallback de conexión.
- `eventlib.infra.outbox.*`: Transactional Outbox.
- `eventlib.infra.resilience.*`: política común de resiliencia.
- `spring.rabbitmq.*`: conexión AMQP y TLS.
- `spring.data.redis.*`: conexión Redis (fuente preferida).
- `spring.r2dbc.*`: conexión a base de datos para el Outbox.

### 8.2 RabbitMQ (`eventlib.infra.rabbitmq`)

| Propiedad | Descripción | Defecto |
|---|---|---|
| `exchange` | Exchange por defecto | `eventlib.exchange` |
| `declare-infrastructure` | Declara la topología al arrancar; si falla, el arranque falla | `true` |
| `declare-infrastructure-overrides.<alias>` | Override de declaración por alias | — |
| `queues.<alias>` | Cola física por alias | vacío |
| `bindings.<alias>` | Routing key por alias | `eventName` |
| `exchanges.<alias>` | Exchange por alias | `exchange` |
| `exchange-types.<alias>` | `TOPIC` o `X_CONSISTENT_HASH` | `TOPIC` |
| `shards.<alias>` | Número de shards para consistent-hash | `4` |
| `virtual-host` | Virtual host usado en validación de topología | `/` |
| `exchange-virtual-hosts.<alias>` | Virtual host por alias (debe coincidir con el configurado) | — |
| `exchange-bindings.<nombre>.source` | Exchange origen del binding exchange-to-exchange | — |
| `exchange-bindings.<nombre>.destination` | Exchange destino | — |
| `exchange-bindings.<nombre>.routing-key` | Routing key del binding | — |
| `exchange-bindings.<nombre>.source-virtual-host` | Virtual host del exchange origen | `virtual-host` |
| `exchange-bindings.<nombre>.destination-virtual-host` | Virtual host del exchange destino | `virtual-host` |
| `exchange-bindings.<nombre>.enabled` | Activa el binding | `true` |
| `dlq.suffix` | Sufijo de la cola DLQ | `.dlq` |
| `queue-type` | `classic` o `quorum` | `classic` |
| `single-active-consumer` | Activa `x-single-active-consumer` en colas TOPIC | `false` |
| `max-queue-length` | Límite de mensajes; overflow `reject-publish` | — |
| `message-ttl` | TTL de mensajes (`x-message-ttl`) | — |
| `consumer-prefetch` | Prefetch del consumidor | `1` |
| `consumer-retry-delay` | Espera antes de reencolar errores transitorios | `2s` |
| `publisher-confirm-timeout-seconds` | Compatibilidad con el constructor legacy del publisher | `10` |
| `publisher-max-retries` | Compatibilidad con el constructor legacy del publisher | `3` |

> Con la auto-configuración actual, el timeout y los reintentos del publicador se controlan con `eventlib.infra.resilience.*`. `publisher-confirm-timeout-seconds` y `publisher-max-retries` solo afectan al constructor legacy de `RabbitMQPublisher`; se mantienen por compatibilidad.

### 8.3 Enrutamiento avanzado

**Por evento:** define `exchanges`, `bindings` y, si el servicio también declara la cola, `queues`. Las claves son alias. Para que los overrides por evento afecten la publicación, el alias debe ser igual a `getEventName()`; si no, se usan `exchange` global y `routingKey = eventName`.

**Exchange consistente-hash:**

```properties
eventlib.infra.rabbitmq.exchange-types.order_created=X_CONSISTENT_HASH
eventlib.infra.rabbitmq.shards.order_created=4
eventlib.infra.rabbitmq.exchanges.order_created=orders.hash.exchange
eventlib.infra.rabbitmq.bindings.order_created=orders.order.v1.created
eventlib.infra.rabbitmq.queues.order_created=orders.order.v1.created.queue
```

La declaración crea las colas `<queue>-0..N-1` con peso `10` y una DLQ en el exchange por defecto. La routing key es `aggregateId`; si el evento no lo define, se usa `eventId` (sin garantía de orden entre eventos distintos).

**Bindings exchange-to-exchange:** requiere el mismo virtual host en origen y destino, y que coincida con el virtual host configurado. Si no, el arranque falla.

### 8.4 Redis e idempotencia (`eventlib.infra.redis`)

| Propiedad | Descripción | Defecto |
|---|---|---|
| `enabled` | Habilita la auto-configuración de Redis | `true` |
| `host` / `port` / `password` | Fallback de conexión si no existe `spring.data.redis.*` | `localhost` / `6379` / — |
| `ssl-enabled` | TLS en el fallback de conexión | `false` |
| `idempotency-prefix` | Prefijo de claves | `eventlib:idempotency` |
| `idempotency-ttl` | TTL del estado `COMPLETED` | `24h` |
| `claim-ttl` | TTL del claim `IN_PROGRESS`; se renueva cada `claim-ttl/3` | `30s` |
| `allow-no-op` | Habilita el store nulo **solo para pruebas** | `false` |

La clave es `<idempotency-prefix>:<eventId>`; `eventId` admite hasta 256 caracteres. Redis es la fuente de verdad para idempotencia y, si está disponible y `lock-enabled=true`, también para el lease líder del Outbox.

> Si Spring Boot no crea un `RedisConnectionFactory` (no hay `spring.data.redis.*`), EventLib usa el fallback `eventlib.infra.redis.*` y registra un `WARN`. No uses el fallback como configuración principal en producción.

### 8.5 Transactional Outbox (`eventlib.infra.outbox`)

| Propiedad | Descripción | Defecto |
|---|---|---|
| `enabled` | Activa el Outbox | `false` |
| `batch-size` | Filas por ciclo | `100` |
| `polling-interval` | Intervalo de polling en milisegundos | `5000` |
| `lock-enabled` | Lease líder en Redis para que una sola instancia procese por ciclo | `true` |
| `lock-ttl` | TTL del lease; se renueva cada `lock-ttl/3` | `60s` |
| `max-attempts` | Intentos antes de `DEAD` | `5` |
| `retry-backoff` | Backoff base | `3s` |
| `max-backoff` | Tope del backoff exponencial | `5m` |
| `require-transaction` | Exige transacción R2DBC activa al guardar el evento | `true` |

### 8.6 Resiliencia (`eventlib.infra.resilience`)

| Propiedad | Descripción | Defecto |
|---|---|---|
| `timeout` | Timeout por intento | `10s` |
| `retry.max-attempts` | Intentos totales (1 = sin reintentos) | `3` |
| `retry.backoff` | Backoff inicial | `500ms` |
| `retry.max-backoff` | Backoff máximo | `10s` |
| `retry.jitter` | Jitter (0..1) | `0.2` |
| `circuit-breaker.enabled` | Activa circuit breaker | `true` |
| `circuit-breaker.failure-rate-threshold` | Umbral de fallo (%) | `50` |
| `circuit-breaker.sliding-window-size` | Ventana deslizante | `20` |
| `circuit-breaker.minimum-number-of-calls` | Llamadas mínimas | `10` |
| `circuit-breaker.wait-duration-in-open-state` | Espera en abierto | `10s` |
| `circuit-breaker.permitted-calls-in-half-open-state` | Llamadas en semiabierto | `3` |
| `bulkhead.enabled` | Activa bulkhead | `true` |
| `bulkhead.max-concurrent-calls` | Llamadas concurrentes máximas | `25` |
| `bulkhead.max-wait-duration` | Espera máxima en bulkhead | `100ms` |
| `reconnect-delay` | Espera entre reconexiones al broker | `5s` |
| `shutdown-timeout` | Espera para drenar trabajo al apagar | `20s` |

### 8.7 Personalización de beans

Puedes reemplazar los componentes principales definiendo tus propios beans, ya que la auto-configuración usa `@ConditionalOnMissingBean`:

- `EventPublisher` (publicación directa o propia).
- `MessagePublisher` (transporte alternativo).
- `MessageSerializer` (serialización propia).
- `EventRoutingResolver` (rutas dinámicas).
- `HashKeyResolver` (clave de shard para consistent-hash).
- `IdempotencyStore` (almacén de idempotencia propio).
- `ObjectMapper` (configuración Jackson de la aplicación).

### 8.8 Variables de entorno

No incluyas credenciales ni certificados en el repositorio:

```env
DB_USER=...
DB_PASSWORD=...
RABBIT_USER=...
RABBIT_PASSWORD=...
RABBITMQ_HOST=localhost
RABBITMQ_PORT=5671
REDIS_HOST=localhost
REDIS_PORT=6379
```

## 9. Uso: productor

### 9.1 Publicación directa

Con `eventlib.infra.outbox.enabled=false` (o ausente), se registra `DirectEventPublisher`:

```java
public Mono<Void> publish(User user) {
    return eventPublisher.publish(new UserRegisteredEvent(user.getId(), user.getEmail()));
}
```

El evento se serializa y publica de inmediato. Si el broker rechaza, no confirma o no hay binding, el `Mono` termina en error.

### 9.2 Publicación transaccional

Con `eventlib.infra.outbox.enabled=true`, el bean `EventPublisher` es `OutboxEventPublisher`:

```java
@Transactional
public Mono<Void> register(User user) {
    return userRepository.save(user)
            .then(eventPublisher.publish(new UserRegisteredEvent(user.getId(), user.getEmail())));
}
```

Alternativa recomendada, sin depender de que el llamador abra la transacción:

```java
public Mono<User> register(User user) {
    return outboxPublisher.executeAndPublish(
            userRepository.save(user),
            saved -> new UserRegisteredEvent(saved.getId(), saved.getEmail()));
}
```

`executeAndPublish` requiere un `TransactionalOperator` (R2DBC). Si no existe, devuelve error explícito.

### 9.3 Eventos con orden por agregado

```java
public class OrderUpdatedEvent extends IntegrationEvent {
    public OrderUpdatedEvent() { super(); }

    public OrderUpdatedEvent(String orderId) {
        super();
        setAggregateId(orderId);
    }

    @Override
    public String getEventName() {
        return "orders.order.v1.updated";
    }
}
```

Con `X_CONSISTENT_HASH`, todos los eventos del mismo `aggregateId` van al mismo shard.

## 10. Uso: consumidor

### 10.1 Handler

Un handler por `eventName`. Registrar dos handlers con el mismo nombre provoca un error de contexto.

```java
@Component
public class OrderCreatedHandler implements IntegrationEventHandler<OrderCreatedEvent> {

    @Override
    public String getEventName() {
        return "orders.order.v1.created";
    }

    @Override
    public Mono<Void> handle(OrderCreatedEvent event) {
        return process(event)
                .onErrorMap(BusinessRuleException.class,
                        error -> new PermanentEventException("Evento inválido", error))
                .onErrorMap(DatabaseUnavailableException.class,
                        error -> new TransientEventException("Base de datos no disponible", error));
    }
}
```

### 10.2 Suscripción

El consumo se inicia explícitamente. Usa el puerto `MessageReceiver` para no acoplarte a RabbitMQ:

```java
@Bean
public ApplicationRunner subscribe(MessageReceiver receiver) {
    return args -> receiver
            .consume("orders.order.v1.created.queue", OrderCreatedEvent.class)
            .subscribe();
}
```

No uses `onErrorContinue` en este flujo: absorbería el error que activa la reconexión interna.

### 10.3 Clasificación de errores

- Lanza `PermanentEventException` para errores de negocio irrecoverables.
- Lanza `TransientEventException` para fallos recuperables.
- Cualquier otra excepción se trata como transitoria para evitar pérdida silenciosa.
- Los errores de deserialización se envían a la DLQ.

## 11. Idempotencia

### 11.1 Contrato

```java
Mono<ClaimResult> tryClaim(String eventId, String owner, Duration ttl);
Mono<Boolean> renewClaim(String eventId, String owner, Duration ttl);
Mono<Boolean> markCompleted(String eventId, String owner);
Mono<Boolean> releaseClaim(String eventId, String owner);
```

`ClaimResult`:

- `CLAIMED`: esta entrega es la dueña.
- `IN_PROGRESS`: otra entrega lo está procesando.
- `COMPLETED`: ya se procesó correctamente.

El `owner` es único por entrega (`instancia:UUID`) y actúa como fencing token: solo el dueño vigente puede renovar, completar o liberar.

### 11.2 Implementación Redis

`ReactiveRedisIdempotencyStore` usa scripts Lua atómicos:

- `tryClaim`: `SET NX PX` atómico.
- `renewClaim`: `PEXPIRE` solo si el owner coincide.
- `markCompleted`: `SET COMPLETED` solo si el owner coincide.
- `releaseClaim`: `DEL` solo si el owner coincide.

Valores: `IN_PROGRESS:<owner>` y `COMPLETED`.

### 11.3 Semántica de duplicados

- Duplicado `COMPLETED`: se ACKea sin ejecutar el handler.
- Claim `IN_PROGRESS`: se reencola.
- Claim perdido durante el handler: el heartbeat emite error, el handler se cancela, se libera el claim y la entrega se reencola.
- `markCompleted` devuelve `false`: el claim expiró o cambió de dueño; se reencola.
- Fallo de Redis: la entrega se reencola; nunca se ACKea.

### 11.4 Fail-closed

Si hay handlers registrados y no existe un `IdempotencyStore`, el contexto falla. El store nulo solo se crea con `eventlib.infra.redis.allow-no-op=true` y está pensado únicamente para pruebas. En producción debe permanecer en `false`.

## 12. Transactional Outbox

### 12.1 Esquema

El Outbox incluido es específico de PostgreSQL: el claim usa `NOW()`, `make_interval(...)` y `UPDATE ... RETURNING`. Las migraciones de referencia son:

- `V1__create_outbox_events.sql`: tabla e índices base.
- `V2__outbox_hardening.sql`: `event_id` como `VARCHAR(256)`, índices de lease y de `event_id`.

Columnas relevantes: `id`, `event_id`, `event_name`, `topic`, `exchange`, `routing_key`, `payload`, `created_at`, `published_at`, `class_name`, `status`, `attempts`, `next_attempt_at`, `locked_by`, `locked_until`, `last_error`.

### 12.2 Máquina de estados

```text
PENDING ──claim──> PROCESSING ──publish ok──> PUBLISHED
                          │
                          ├──error──> FAILED ──backoff vencido──> PROCESSING
                          └──intentos agotados──> DEAD
```

- `DEAD` no se reintenta automáticamente.
- `PROCESSING` con `locked_until` vencido se recupera (tras una caída).
- `FAILED` se reintenta cuando `next_attempt_at <= NOW()`.
- `PUBLISHED` no se vuelve a procesar.

### 12.3 Claim y lease

- Cada intento usa un token único (`owner:UUID`).
- El claim es atómico y usa el reloj de la base de datos.
- El lease se renueva cada `lock-ttl/3` mientras se publica.
- La publicación se marca solo si el token sigue siendo el dueño.
- Si se pierde el lease, otro worker puede republicar: garantía at-least-once.

### 12.4 Scheduler y escala

- `@Scheduled` con `polling-interval`; no hay ciclos solapados en una instancia.
- Con `lock-enabled=true` y Redis disponible, solo una instancia del servicio procesa por ciclo.
- Si no existe `ReactiveStringRedisTemplate` (por ejemplo, con `eventlib.infra.redis.enabled=false`), el scheduler no usa lock y cada instancia procesa su propio Outbox.
- Si el lock está habilitado pero Redis no responde, el ciclo falla y no se publica nada (fail-closed). Restaura Redis o desactiva `lock-enabled`; el claim atómico por fila sigue evitando que dos workers procesen la misma fila.
- Al apagar, espera `shutdown-timeout`; lo que quede en `PROCESSING` se recupera al vencer el lease.

### 12.5 Operación

Para reintentar un evento `DEAD` manualmente:

```sql
UPDATE outbox_events
SET status = 'PENDING', attempts = 0, next_attempt_at = NULL,
    locked_by = NULL, locked_until = NULL, last_error = NULL
WHERE id = :id;
```

Recomendaciones:

- Migrar el esquema antes de activar el Outbox.
- Monitorizar `eventlib.outbox.pending` y los estados `FAILED`/`DEAD`.
- Mantener `require-transaction=true` salvo que se acepte perder atomicidad.
- Dimensionar `lock-ttl` por encima del peor caso de publicación del lote.

## 13. RabbitMQ, TLS y topología

### 13.1 Conexión

EventLib crea su propio `ConnectionFactory` a partir de `spring.rabbitmq.*` y usa conexiones separadas para productor y consumidor. La recuperación automática del cliente AMQP está desactivada; la reconexión la gestiona `RabbitConnectionProvider` de forma reactiva con `reconnect-delay`, circuit breaker y bulkhead.

### 13.2 TLS

```properties
spring.rabbitmq.ssl.enabled=true
spring.rabbitmq.ssl.verify-hostname=true
spring.rabbitmq.ssl.trust-store=file:/run/secrets/rabbitmq-ca.pem
# mTLS opcional:
# spring.rabbitmq.ssl.key-store=file:/run/secrets/client.p12
# spring.rabbitmq.ssl.key-store-type=PKCS12
# spring.rabbitmq.ssl.key-store-password=${KEYSTORE_PASSWORD}
```

- Trust store: PEM/CRT o JKS/PKCS12.
- Key store de cliente: JKS/PKCS12 (PEM no soportado para key store).
- Sin trust store se usa el trust store por defecto de la JVM.
- La validación de cadena no se desactiva; `validate-server-certificate=false` no es una opción soportada en producción.
- `verify-hostname=true` habilita la verificación de hostname. Si el certificado no coincide con el host, corrige el certificado o el nombre; no desactives la validación.

### 13.3 Declaración de topología

Al arrancar se declaran el exchange por defecto, los exchanges por ruta, las colas durables, sus bindings y las DLQ. Si `RabbitAdmin.initialize()` falla, el arranque falla con un error explícito. No se admite arranque degradado.

Para colas TOPIC con DLQ:

- Cola: `<queue>` con `x-dead-letter-exchange=<exchange>` y `x-dead-letter-routing-key=<routingKey>.dlq`.
- DLQ: `<queue>.dlq` enlazada al mismo exchange con `<routingKey>.dlq`.

### 13.4 Producción

- Usa `eventlib.infra.rabbitmq.queue-type=quorum` para colas quorum.
- Acota con `max-queue-length` y `message-ttl`.
- Mantén `verify-hostname=true` y un trust store explícito.
- Revisa que `getEventName()`, routing keys y bindings coincidan en productor y consumidor.

## 14. Observabilidad

### 14.1 Métricas

Con un `MeterRegistry` disponible:

| Métrica | Etiquetas | Descripción |
|---|---|---|
| `eventlib.resilience.retries` | `name` | Reintentos ejecutados |
| `eventlib.resilience.failures` | `name`, `exception` | Fallos de la política |
| `eventlib.publish` | `outcome` | Resultado de publicación (`ack`, `nack`, `unroutable`, `timeout`, etc.) |
| `eventlib.outbox.transitions` | `status` | Transiciones del Outbox (`published`, `failed`, `dead`, `lease_lost`) |
| `eventlib.outbox.pending` | — | Gauge de eventos pendientes |
| `eventlib.circuitbreaker.state` | `name` | Estado del circuit breaker |
| `eventlib.rabbit.reconnects` | `type` | Reconexiones (`PRODUCER`/`CONSUMER`) |

### 14.2 Health

Con Actuator en el classpath se registra `eventLibHealthIndicator` (nombre de bean). Reporta:

- exchange configurado,
- tipo de cola,
- si el Outbox está habilitado,
- si el store nulo está permitido,
- estado de Redis mediante `PING` con timeout de 2 segundos.

RabbitMQ no se consulta en el health check para no acoplar la salud de la aplicación al broker; el publisher ya falla cerrado por confirmación.

## 15. Build, pruebas, calidad y release

### 15.1 Comandos

```bash
# Pruebas unitarias
mvn -f eventlib/pom.xml test

# Verificación completa (incluye Failsafe si existen *IT)
mvn -f eventlib/pom.xml verify

# Cobertura (mínimo 50% de instrucciones del bundle)
mvn -f eventlib/pom.xml verify -Pquality

# Análisis de vulnerabilidades (falla con CVSS >= 7)
mvn -f eventlib/pom.xml verify -Psecurity

# Instalación local consumida por los servicios
mvn -f eventlib/pom.xml clean install -DskipTests -Dgpg.skip=true

# Compilación de servicios desde la raíz del repositorio
mvn -q -pl auth-services,email-service -am -DskipTests compile
```

### 15.2 Reportes y SBOM

- JaCoCo genera el reporte en `target/site/jacoco/`.
- CycloneDX genera el SBOM en `target/bom.json` durante `package`.
- Surefire deja los reportes en `target/surefire-reports/`.

### 15.3 CI

`../contextoInicial/.github/workflows/ci.yml` ejecuta:

1. `mvn -B -f eventlib/pom.xml clean verify -Dgpg.skip=true`.
2. Instalación local del starter.
3. Compilación de `auth-services` y `email-service`.
4. Pruebas de `auth-services`.
5. Publicación de reportes (Surefire, JaCoCo, SBOM) como artefactos.

Los perfiles `quality` y `security` no se ejecutan en CI actualmente.

### 15.4 Release

El perfil `release` activa GPG y `central-publishing-maven-plugin`:

```bash
mvn -f eventlib/pom.xml clean deploy -Prelease
```

Requisitos: clave GPG privada disponible, `settings.xml` con el servidor `central` y namespace verificado. `autoPublish=false`, por lo que la publicación requiere aprobación posterior en Central. GPG y Central no se ejecutan en `verify` normal.

## 16. Migración desde 1.0.0

- `MessageSerializer` requiere el `ObjectMapper` de la aplicación (no crea uno aislado).
- La idempotencia es fail-closed; el store nulo solo se habilita con `allow-no-op=true`.
- La declaración de infraestructura RabbitMQ falla el arranque si no puede completarse.
- El `ConnectionFactory` se comparte entre Reactor y Spring AMQP.
- Se eliminó `RabbitMessageRecoverer` para evitar bucles de recuperación.
- El Outbox incorpora columnas de claim, estados `FAILED`/`DEAD`, backoff y lease renovable.
- GPG y publicación en Central se movieron al perfil `release`.

Si vienes de `1.0.0`, aplica las migraciones `V1` y `V2` y revisa que tu esquema incluya todas las columnas de claim.

## 17. Solución de problemas

| Síntoma | Causa probable | Acción |
|---|---|---|
| `No existe handler para el evento ...` en logs y el mensaje va a DLQ | No hay `IntegrationEventHandler` para ese `eventName` | Registra el handler o corrige `getEventName()` |
| `Hay más de un handler registrado para el evento ...` al arrancar | Dos handlers con el mismo `eventName` | Deja uno solo |
| Error al arrancar por `IdempotencyStore es obligatorio` | Hay handlers y no hay store | Configura Redis o habilita `allow-no-op` solo en pruebas |
| `OutboxEventPublisher.publish() requiere una transacción R2DBC activa` | `require-transaction=true` sin transacción | Usa `@Transactional`, `TransactionalOperator` o `executeAndPublish` |
| `Mensaje no enrutable` | No hay cola vinculada al exchange/routing key | Revisa `bindings`, `queues` y la topología declarada |
| Arranque falla al declarar RabbitMQ | Broker/certs/vhost incorrectos o topología inválida | Revisa `spring.rabbitmq.*`, TLS y bindings; el fail-fast es intencional |
| El consumidor no recibe eventos | `eventName`, routing key o cola no coinciden | Verifica ambos lados y la cola suscrita |
| Filas `PROCESSING` atascadas | Worker caído | Espera el vencimiento del lease o revisa `locked_until` |
| Filas `DEAD` | Intentos agotados | Corrige la causa y reintenta manualmente el estado |
| Outbox no publica y aparecen errores `Error en ciclo de Outbox` | Lock habilitado y Redis no responde | Restaura Redis o usa `lock-enabled=false` |
| `No se pudo configurar TLS de RabbitMQ` | Trust store/key store inválido o inexistente | Revisa rutas, tipo y contraseñas; usa PKCS12/JKS para key store |
| Outbox no procesa | `enabled=false`, sin R2DBC o sin scheduler | Activa el Outbox, revisa `spring.r2dbc.*` y el polling |

## 18. Seguridad y licencia

- Reporta vulnerabilidades de forma privada según `SECURITY.md`; no abras un issue público.
- No publiques credenciales ni certificados en el repositorio.
- En producción: TLS con trust store explícito, `verify-hostname=true`, Redis habilitado, `allow-no-op=false`, colas quorum y DLQ acotadas.
- Licencia: Apache License 2.0. Consulta `LICENSE` y `NOTICE`.

---

EventLib `1.0.1` — documentación mantenida junto al código. Para cambios recientes, consulta `CHANGELOG.md`.
