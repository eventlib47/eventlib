# Security policy

## Supported versions

Security fixes are applied to the latest `1.x` release line. Older
snapshots or forks are not supported.

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability. Report it
privately to the maintainers with:

- affected version or commit,
- a minimal reproduction,
- impact and attack preconditions,
- any proposed fix.

You should receive an acknowledgement within 72 hours. Please allow time
for a coordinated disclosure before publishing details.

## Production baseline

- RabbitMQ must use TLS (`spring.rabbitmq.ssl.enabled=true`) with an
  explicit trust store. `validate-server-certificate=false` is not a
  supported production setting.
- Redis idempotency must be enabled. The no-op store is only available
  when `eventlib.infra.redis.allow-no-op=true` and must never be used in
  production.
- Outbox rows must use a PostgreSQL schema that contains the claim columns
  (`status`, `attempts`, `next_attempt_at`, `locked_by`, `locked_until`,
  `last_error`, `exchange`, `routing_key`).
- Consumer handlers must classify irrecoverable failures with
  `PermanentEventException`; everything else is retried and can reach the
  DLQ only through an explicit permanent classification.
- Credentials and certificates must be supplied by the deployment
  environment, never committed to the repository.
