# Changelog

All notable changes to EventLib are documented here. The project follows
semantic versioning.

## [Unreleased]

_No unreleased changes._

## [1.0.1] - 2026-11-16

### Added

- Claim/complete/release idempotency contract with fencing owner and atomic
  Redis Lua scripts.
- `PermanentEventException` and `TransientEventException` for explicit
  consumer error classification.
- Outbox claim columns, conditional publishing, exponential backoff,
  `FAILED`/`DEAD` states and a renewable Redis lease.
- Persistent RabbitMQ message properties (`messageId`, `type`, `appId`,
  `correlationId`, delivery mode 2).
- TLS trust store/key store support through `RabbitSslSupport`.
- Configurable queue type, bounded queues, message TTL, consumer prefetch
  and publisher retries.
- Unit tests for dispatch, serialization, ACK/NACK decisions, publisher
  properties and infrastructure declaration.
- CI workflow, Maven quality profiles, JaCoCo report and CycloneDX SBOM.

### Changed

- `MessageSerializer` now requires an application `ObjectMapper`.
- `IdempotencyStore` is fail-closed; the no-op store is opt-in only.
- RabbitMQ infrastructure declaration fails fast at startup.
- Rabbit `ConnectionFactory` is shared between Reactor and Spring AMQP.
- GPG signing and Central publishing moved to the `release` profile.

### Removed

- Unused `RabbitMessageRecoverer`, which moved DLQ messages back to the
  original exchange and could create an infinite recovery loop.

## [1.0.0]

Initial starter release.
