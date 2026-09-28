# Webhook Ingestion with Replay and DLQ

> Status: 🚧 planned — not yet implemented

## Goal

Reliably receive events from external partners, even when the partner resends, delays, or fails.

## Features

- Webhook received and published to Kafka
- Idempotent consumer — the same event key processed twice does not duplicate the effect
- Browsable dead letter queue for events that failed
- Endpoint to reprocess events from the DLQ
- Incremental cursor-based sync, with a full resync when the cursor is too stale

## Stack

Node.js/TypeScript, Kafka, Redis for dedupe.

## Kubernetes

Optional here. Kafka already runs as a cluster; the project's value is in the idempotency and replay logic, not the orchestration.

## How to run

Locally, without Docker:

```bash
npm install
npm run infra:up      # starts Postgres, Redis, Kafka
npm run migrate       # applies the database schema
npm run mock-partner-api   # terminal 1 — synthetic partner events on port 4002
npm run dev                 # terminal 2 — main service on port 3000
```

With Docker Compose (everything, including the app itself):

```bash
docker compose up --build
```

Testing:

```bash
curl -X POST http://localhost:3000/webhooks/payments \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt_demo_1","paymentId":"pay_demo_1","type":"payment.succeeded"}'

curl -X POST http://localhost:3000/webhooks/payments \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt_demo_2","paymentId":"pay_fail_demo","type":"payment.succeeded"}'

curl http://localhost:3000/dlq
```

The second call uses the magic failure trigger (`pay_fail_demo`), which always fails processing and lands in the DLQ after 3 retries — use it to demonstrate the DLQ and reprocess flow on demand.

Reprocessing a DLQ entry:

```bash
curl -X POST http://localhost:3000/dlq/<id>/reprocess
```

Triggering reconciliation on demand:

```bash
curl -X POST http://localhost:3000/reconciliation/run
```

Running the tests (requires `npm run infra:up` and `npm run migrate` first):

```bash
npm test
npm run typecheck
npm run lint
```
