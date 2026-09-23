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

## What it demonstrates

Real understanding of delivery guarantees in distributed systems: at-least-once, idempotency, safe replay.
