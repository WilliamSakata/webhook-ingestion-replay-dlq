# Webhook Ingestion with Replay and DLQ — Design

Date: 2026-09-28
Status: Approved

## Goal

Reliably ingest webhook events from an external payments partner, even when
the partner resends, delays, or occasionally fails to deliver at all. The
project demonstrates at-least-once delivery handling, idempotent
processing, safe replay from a dead letter queue, and cursor-based
reconciliation against the partner's own event history — the guarantees a
production webhook consumer needs, not just the vocabulary for them.

## Scope

In scope:

- An HTTP endpoint that receives a partner webhook and publishes it to
  Kafka (the ingestion path never processes the event itself — it only
  accepts and forwards, so it can ack the partner quickly).
- A Kafka consumer that processes payment events with retry-then-DLQ
  behavior and two-layer idempotency (a Redis dedupe cache plus a
  naturally idempotent upsert as the correctness backstop).
- A dead letter queue, stored in Postgres (not a second Kafka topic) so it
  is trivially browsable and queryable via HTTP.
- An HTTP API to list DLQ entries and reprocess one by republishing it to
  the same Kafka topic the live path uses — there is exactly one
  processing path in this system, never a parallel one for replays.
- A cursor-based reconciliation job against a mock partner "list events"
  API, with incremental sync when the cursor is fresh and a full resync
  when it is missing or stale. Reconciliation publishes to Kafka too, for
  the same single-path reason.
- Local environment: Kafka (KRaft mode, no ZooKeeper), Redis, PostgreSQL,
  all via Docker Compose.

Out of scope:

- A separate Kafka retry-topic pattern (delay topics, exponential
  redelivery via topic hierarchy). Retries happen in-process inside the
  consumer's message handler; the design section below explains the
  trade-off this accepts.
- Kubernetes manifests. Kafka already needs real infrastructure locally;
  the project's value is the idempotency/replay/reconciliation logic, not
  orchestration.
- Authentication/signature verification on the inbound webhook endpoint
  (a real integration would verify an HMAC signature from the partner;
  omitted here to keep focus on delivery guarantees).

## Architecture

Hexagonal-lite, consistent with the portfolio's `kyc-risk-decision-engine`
project, but with a thinner domain layer — this project's interesting
logic is about delivery guarantees, not business rules:

```
webhook-ingestion-replay-dlq/
  src/
    domain/
      value-objects/
        PaymentEvent.ts          # { eventId, paymentId, type, status, receivedAt }
      services/
        PaymentEventValidator.ts # pure validation of the raw webhook payload shape
    application/
      ports/
        EventPublisherPort.ts     # publish(event): Promise<void>
        PaymentRepositoryPort.ts  # upsertPaymentStatus(paymentId, status): Promise<void>
        DlqRepositoryPort.ts      # add / list / get / markReprocessed
        DedupeStorePort.ts        # wasProcessed(eventId) / markProcessed(eventId)
        PartnerEventsApiPort.ts   # listEvents(cursor, limit): Promise<{ events, nextCursor }>
        ReconciliationCursorPort.ts # load / save
      use-cases/
        IngestWebhookEvent.ts      # validate shape, publish to Kafka, return immediately
        ProcessPaymentEvent.ts     # dedupe check -> handle -> upsert+mark, or retry-then-DLQ
        ReprocessDlqEvent.ts       # load DLQ row, republish, mark reprocessed
        ReconcilePayments.ts       # cursor-based catch-up, republishes missed events
    adapters/
      inbound/http/
        webhookRouter.ts           # POST /webhooks/payments
        dlqRouter.ts                # GET /dlq, POST /dlq/:id/reprocess
        reconciliationRouter.ts     # POST /reconciliation/run
        app.ts
      inbound/kafka/
        paymentEventConsumer.ts     # wires ProcessPaymentEvent to a KafkaJS consumer
      outbound/kafka/
        KafkaEventPublisher.ts      # implements EventPublisherPort via KafkaJS producer
      outbound/postgres/
        PostgresPaymentRepository.ts
        PostgresDlqRepository.ts
        PostgresReconciliationCursorRepository.ts
      outbound/redis/
        RedisDedupeStore.ts
      outbound/partner-api/
        HttpPartnerEventsApiClient.ts
    config/
      env.ts
    main.ts                         # composition root: HTTP server + Kafka consumer + reconciliation interval
  mock-partner-api/
    server.ts                       # exports createMockPartnerApiApp(); standalone via require.main guard
  db/
    migrations/                     # plain SQL, applied via a small migration runner at startup
  docker-compose.yml
  Dockerfile
```

`eslint-plugin-boundaries` enforces the same layering rule as the KYC
project: `domain` only imports `domain`; `application` imports
`domain`+`application`; `adapters`/`main` may import everything inward.

## Data model (PostgreSQL)

```sql
CREATE TABLE payments (
  payment_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE dlq_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  failure_reason TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reprocessed_at TIMESTAMPTZ
);

CREATE TABLE reconciliation_cursor (
  id TEXT PRIMARY KEY,
  cursor TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);
```

`payments` is written only via `INSERT ... ON CONFLICT (payment_id) DO
UPDATE` — this upsert is the "naturally idempotent effect" backstop:
applying the same status twice converges to the same row, so even if the
Redis dedupe layer misses (TTL expiry, a lost write, a race), the stored
state is still correct.

## Idempotent consumption

**The Redis dedupe key is written only after successful processing, never
before.** This ordering is deliberate:

1. `GET dedupe:{eventId}` — if present, this event was already
   successfully processed; skip (ack immediately, no DB write).
2. If absent, attempt processing (validate → check the failure trigger →
   upsert into `payments`).
3. Only on success: upsert `payments`, then `SET dedupe:{eventId} 1 EX
   86400`.
4. On failure, the Redis key is never set.

If a dedupe key were claimed *before* processing (the common `SET NX`
optimistic-lock pattern), a permanently-failing event would "poison"
itself: every retry attempt, and any later DLQ reprocess, would see the
key already claimed and skip processing forever — without the event ever
having actually succeeded. Writing the key only after success avoids this,
at the cost of a small window where two concurrent consumers could both
attempt the same event (acceptable for a single-consumer-instance demo;
the upsert's own idempotency still makes this safe even if it happens).

## Failure handling: retry then DLQ

A message fails processing when either:

- The payload does not match the expected shape (a real "partner sent a
  malformed event" case), or
- `paymentId === "pay_fail_demo"` — a magic trigger so a failure can be
  demonstrated on demand, the same technique the KYC project used with
  `cus_sanctioned`.

On failure, `ProcessPaymentEvent` retries in-process up to 3 times with
exponential backoff (100ms, 200ms, 400ms) before giving up. If all
retries fail, the event is inserted into `dlq_events` with the failure
reason and attempt count, and the consumer commits the offset and moves
on to the next message.

**Trade-off, stated explicitly:** retrying in-process blocks that Kafka
partition for the duration of the retries. A production system with high
failure rates would use a separate retry-topic-with-delay pattern
(publish to `payment-events-retry-1`, consume after a delay, escalate to
`-retry-2`, etc.) to avoid blocking healthy messages behind a failing one.
That pattern is deliberately out of scope here — it adds real complexity
for a portfolio project whose point is demonstrating retry-then-DLQ
semantics and idempotency, not building a production-grade retry
scheduler.

## Reprocessing from the DLQ

`POST /dlq/:id/reprocess` loads the stored payload and republishes it to
the same `payment-events` Kafka topic, with the same event key. It then
flows through the exact same `ProcessPaymentEvent` path as a live webhook
— including the dedupe check. The DLQ row is marked `reprocessed_at` (not
deleted), so it stays in the audit trail but is excluded from `GET /dlq`'s
default (unresolved-only) listing.

## Reconciliation (cursor-based sync)

A standalone mock partner API (`mock-partner-api/server.ts`, exporting
`createMockPartnerApiApp()` the same way `kyc-risk-decision-engine`'s mock
server does) serves a fixed, seeded list of synthetic payment events via
`GET /events?cursor=&limit=`, paginated by an integer cursor over its
in-memory list. These events represent activity the partner has that this
system's webhook may have missed.

`ReconcilePayments`:

1. Loads the saved cursor from `reconciliation_cursor`.
2. If the cursor is missing, or its `updated_at` is older than 24h (the
   "too stale" threshold), performs a **full resync**: walks the partner
   API from the beginning (`cursor=null`), across all pages.
3. Otherwise performs an **incremental sync**: calls the partner API with
   the saved cursor, processing only the newly returned events.
4. For every event returned, republishes it to `payment-events` — the
   reconciliation job is a catch-up publisher only; it never writes to
   `payments` directly.
5. Saves the new cursor and `updated_at`.

Trigger: `POST /reconciliation/run` for on-demand/demo use, plus a
`setInterval` in the composition root (`main.ts`) so it also runs
periodically without manual triggering — no Kubernetes CronJob needed,
consistent with the project's "Kubernetes: optional, not the point"
framing.

## API

- `POST /webhooks/payments` — body is the raw partner event
  (`{ eventId, paymentId, type, status }`); validates shape, publishes to
  Kafka, returns `202 Accepted` immediately (a webhook receiver should
  ack fast and process asynchronously).
- `GET /dlq?limit=&offset=` — lists unresolved DLQ entries
  (`reprocessed_at IS NULL`) with failure reason and attempt count.
- `POST /dlq/:id/reprocess` — republishes the stored event, marks the row
  reprocessed.
- `POST /reconciliation/run` — triggers a reconciliation pass immediately.

## Testing strategy

Same pyramid as `kyc-risk-decision-engine`, adjusted for Kafka's weight:

1. **Domain/application** — `PaymentEventValidator`, `ProcessPaymentEvent`
   (dedupe ordering, retry-then-DLQ, the magic failure trigger),
   `ReconcilePayments` (incremental vs. full-resync branching) — all
   tested against in-memory fakes of every port. No real Kafka, Redis, or
   Postgres. This is the bulk of the suite and runs in milliseconds.
2. **Adapters** — `PostgresPaymentRepository`, `PostgresDlqRepository`,
   `RedisDedupeStore` each get a focused test against a real local
   instance (Docker Compose services used directly in the test run, the
   same way `kyc-risk-decision-engine` tested `HttpRiskVerificationAdapter`
   against a real local HTTP server) — these are the tests that actually
   need the real infrastructure, not a full end-to-end wiring.
3. **Integration** — one thin end-to-end test that boots the real HTTP
   app wired to real Kafka, Redis, and Postgres (via the project's Docker
   Compose services, assumed already running for this test — unlike the
   KYC project, Kafka cannot be faked with a local HTTP server), publishes
   a webhook, and asserts the payment lands in Postgres; a second
   integration test drives the failure trigger through to the DLQ and
   back out via reprocess.

## Local environment

Docker Compose runs: Kafka (KRaft mode, combined broker+controller,
single node — no ZooKeeper), Redis, PostgreSQL, the mock partner API, and
the app itself. `kafkajs` is the Kafka client.

## What it demonstrates

- Real understanding of at-least-once delivery: retries, idempotent
  processing, and a DLQ that is actually inspectable and actionable
  rather than a black hole.
- A correctness-aware idempotency design: dedupe-after-success ordering
  and a naturally idempotent storage layer as a deliberate two-layer
  defense, with the reasoning for that ordering spelled out rather than
  assumed.
- A single processing path shared by live traffic, DLQ replay, and
  reconciliation catch-up — replay isn't a special case bolted on the
  side.
- Judgment about scope: naming the production-grade alternative
  (retry-topic pattern) and explaining why it's deliberately not built
  here, rather than silently under-building without acknowledging the
  gap.
