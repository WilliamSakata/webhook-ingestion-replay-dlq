# Webhook Ingestion with Replay and DLQ Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a hexagonal-lite TypeScript service that ingests partner payment webhooks via Kafka with idempotent processing, retry-then-DLQ failure handling, a browsable/replayable Postgres-backed dead letter queue, and cursor-based reconciliation against a mock partner API — demonstrating real at-least-once delivery guarantees, not just the vocabulary for them.

**Architecture:** Hexagonal-lite (`domain/` → `application/` → `adapters/`), with `eslint-plugin-boundaries` enforcing layering from the first commit. A single Kafka topic (`payment-events`) is the one processing path shared by live webhooks, DLQ replay, and reconciliation catch-up. The DLQ lives in Postgres (not a second Kafka topic) so it is trivially browsable via HTTP. Docker Compose brings up Postgres, Redis, and Kafka (KRaft mode) early, since most of this project's tests need real infrastructure throughout, not just at the end.

**Tech Stack:** Node.js 22, TypeScript (strict, CommonJS), Express, Zod, KafkaJS, `pg` (node-postgres, no ORM), `redis` (official v4 client), Vitest, Supertest, `tsx`, ESLint + `eslint-plugin-boundaries` v7.

**Spec:** `docs/design/specs/2026-09-28-webhook-ingestion-replay-dlq-design.md`

## Global Constraints

- TypeScript `strict: true`. `domain/` and `application/` must never import from `adapters/` — enforced by `eslint-plugin-boundaries`'s `boundaries/dependencies` rule with `import/resolver` configured for `.ts` extensions from the first commit (a prior project shipped this rule silently inert for weeks because the resolver wasn't configured — do not repeat that).
- The Redis dedupe key is written only **after** successful processing, never before — writing it optimistically before processing would permanently "poison" any event that fails, since every retry and DLQ reprocess would then see the key already claimed and skip real processing forever.
- Retries happen **in-process** inside the Kafka consumer's message handler (exponential backoff, default 3 attempts) — not a separate retry-topic pattern. This blocks that partition during retries; it's a deliberate, stated trade-off, not an oversight.
- There is exactly **one** processing path: live webhooks, DLQ reprocessing, and reconciliation catch-up all publish to the same `payment-events` Kafka topic and flow through the same consumer logic. Never build a parallel processing path for replay.
- All external HTTP responses (the mock partner API's response body) are validated with a Zod schema at the adapter boundary, not trusted via a bare type assertion.
- `vitest.config.mts` (not `.ts`) — avoids a native-config-loader warning from newer Vite/Vitest versions when the rest of the project stays CommonJS.
- `Dockerfile` uses `npm ci` (not `npm install`) and a `.dockerignore` excluding `node_modules` is created in the same task as the Dockerfile — without it, `COPY . .` would copy the host's `node_modules` into the Linux/musl container and break `tsx`'s native `esbuild` binary at runtime.
- The inbound HTTP app has a 4-argument Express error-handling middleware from the start (JSON error responses, including for malformed JSON bodies) — do not retrofit this later.
- No `Co-Authored-By` or any AI-attribution trailer in any commit message.
- `vitest.config.mts` sets `fileParallelism: false` — several test files share live Postgres/Kafka/Redis state (`TRUNCATE`s, shared consumer groups); running test files concurrently would race.
- Docker is installed and working in this environment (verified: `docker run hello-world` succeeded). Adapter and integration tasks in this plan are written **and run** against real Postgres/Redis/Kafka via Docker Compose — they are not statically-reviewed-only.

---

### Task 1: Project scaffold, tooling, and domain types

**Files:**
- Create: `package.json`
- Create: `tsconfig.json`
- Create: `vitest.config.mts`
- Create: `.eslintrc.cjs`
- Create: `src/domain/value-objects/PaymentEvent.ts`

**Interfaces:**
- Produces:
  - `PaymentEventType = 'payment.succeeded' | 'payment.failed'`
  - `PaymentEvent { eventId: string; paymentId: string; type: PaymentEventType }`

No behavior to test in this task (pure types and config); verified via `npm run typecheck` and `npm run lint`.

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "webhook-ingestion-replay-dlq",
  "version": "0.1.0",
  "private": true,
  "engines": {
    "node": ">=18"
  },
  "scripts": {
    "dev": "tsx src/main.ts",
    "mock-partner-api": "tsx mock-partner-api/server.ts",
    "migrate": "tsx db/migrate.ts",
    "infra:up": "docker compose up -d postgres redis kafka",
    "infra:down": "docker compose down",
    "test": "vitest run",
    "test:watch": "vitest",
    "typecheck": "tsc --noEmit",
    "lint": "eslint . --ext .ts"
  },
  "dependencies": {
    "express": "^4.19.2",
    "kafkajs": "^2.2.4",
    "pg": "^8.13.0",
    "redis": "^4.7.0",
    "zod": "^3.23.8"
  },
  "devDependencies": {
    "@types/express": "^4.17.21",
    "@types/node": "^22.5.0",
    "@types/pg": "^8.11.10",
    "@types/supertest": "^6.0.2",
    "@typescript-eslint/eslint-plugin": "^7.16.0",
    "@typescript-eslint/parser": "^7.16.0",
    "eslint": "^8.57.0",
    "eslint-import-resolver-node": "^0.3.9",
    "eslint-plugin-boundaries": "^7.2.0",
    "supertest": "^7.0.0",
    "tsx": "^4.16.2",
    "typescript": "^5.5.4",
    "vitest": "^5.0.0"
  }
}
```

- [ ] **Step 2: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "CommonJS",
    "moduleResolution": "Node",
    "lib": ["ES2022"],
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "noEmit": true
  },
  "include": ["src", "mock-partner-api", "db", "tests"]
}
```

- [ ] **Step 3: Create `vitest.config.mts`**

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    environment: 'node',
    include: ['tests/**/*.test.ts'],
    testTimeout: 15000,
    fileParallelism: false,
  },
});
```

- [ ] **Step 4: Create `.eslintrc.cjs`**

```js
module.exports = {
  root: true,
  parser: '@typescript-eslint/parser',
  parserOptions: {
    ecmaVersion: 2022,
    sourceType: 'module',
  },
  plugins: ['@typescript-eslint', 'boundaries'],
  extends: ['eslint:recommended', 'plugin:@typescript-eslint/recommended'],
  env: {
    node: true,
    es2022: true,
  },
  settings: {
    'import/resolver': {
      node: {
        extensions: ['.ts', '.js'],
      },
    },
    'boundaries/elements': [
      { type: 'domain', pattern: 'src/domain/**' },
      { type: 'application', pattern: 'src/application/**' },
      { type: 'adapters', pattern: 'src/adapters/**' },
      { type: 'config', pattern: 'src/config/**' },
      { type: 'main', pattern: 'src/main.ts' },
    ],
  },
  rules: {
    '@typescript-eslint/no-unused-vars': [
      'error',
      { argsIgnorePattern: '^_' },
    ],
    'boundaries/dependencies': [
      2,
      {
        default: 'disallow',
        policies: [
          {
            from: { element: { type: 'domain' } },
            allow: { to: { element: { type: 'domain' } } },
          },
          {
            from: { element: { type: 'application' } },
            allow: { to: { element: { types: { anyOf: ['domain', 'application'] } } } },
          },
          {
            from: { element: { type: 'adapters' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
          {
            from: { element: { type: 'config' } },
            allow: { to: { element: { type: 'config' } } },
          },
          {
            from: { element: { type: 'main' } },
            allow: {
              to: { element: { types: { anyOf: ['domain', 'application', 'adapters', 'config'] } } },
            },
          },
        ],
      },
    ],
  },
};
```

- [ ] **Step 5: Install dependencies**

Run: `npm install`
Expected: installs without errors, creates `package-lock.json` and `node_modules/`.

- [ ] **Step 6: Create the `PaymentEvent` value object**

`src/domain/value-objects/PaymentEvent.ts`:

```ts
export type PaymentEventType = 'payment.succeeded' | 'payment.failed';

export interface PaymentEvent {
  eventId: string;
  paymentId: string;
  type: PaymentEventType;
}
```

- [ ] **Step 7: Verify tooling**

Run: `npm run typecheck`
Expected: exits 0, no errors.

Run: `npm run lint`
Expected: exits 0, no errors.

- [ ] **Step 8: Commit**

```bash
git add package.json package-lock.json tsconfig.json vitest.config.mts .eslintrc.cjs src/
git commit -m "chore: scaffold tooling and domain types"
```

---

### Task 2: Local infrastructure (Postgres, Redis, Kafka) and database migrations

**Files:**
- Create: `docker-compose.yml`
- Create: `db/migrations/001_create_payments.sql`
- Create: `db/migrations/002_create_dlq_events.sql`
- Create: `db/migrations/003_create_reconciliation_cursor.sql`
- Create: `db/migrate.ts`

**Interfaces:**
- Produces: three Postgres tables (`payments`, `dlq_events`, `reconciliation_cursor`) that every later Postgres-backed adapter task depends on. No TypeScript interfaces produced.

This task has no unit tests — it stands up real local infrastructure and verifies it's reachable. Every later task that touches Postgres, Redis, or Kafka depends on this infrastructure already running.

- [ ] **Step 1: Create `docker-compose.yml`**

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: webhook_ingestion
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      interval: 5s
      timeout: 3s
      retries: 20

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 20

  kafka:
    image: apache/kafka:3.8.0
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:29092,CONTROLLER://:9093,PLAINTEXT_HOST://:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"
```

The dual-listener setup (`PLAINTEXT` on `29092` for other containers on the Docker network, `PLAINTEXT_HOST` on `9092` for the host machine) lets tests running directly on the host connect via `localhost:9092` now, and lets the containerized app connect via `kafka:29092` once it's added in the final task.

- [ ] **Step 2: Bring up the infrastructure and wait for readiness**

Run: `docker compose up -d --wait postgres redis`
Expected: both services report healthy (the command blocks until healthchecks pass, or fails after `retries` attempts — if it fails, run `docker compose logs postgres redis` to diagnose before continuing).

Run: `docker compose up -d kafka`
Expected: container starts. Kafka has no compose-level healthcheck (KRaft startup timing varies by image); verify it directly in Step 3 instead.

Run:

```bash
for i in $(seq 1 30); do
  node -e "
    const { Kafka } = require('kafkajs');
    const kafka = new Kafka({ clientId: 'infra-check', brokers: ['localhost:9092'] });
    const admin = kafka.admin();
    admin.connect().then(() => admin.disconnect()).then(() => process.exit(0)).catch(() => process.exit(1));
  " && break
  sleep 2
done
```

Expected: exits 0 within the 30 retries (60 seconds), confirming Kafka accepted an admin connection. If it never succeeds, run `docker compose logs kafka` to diagnose.

- [ ] **Step 3: Create the payments migration**

`db/migrations/001_create_payments.sql`:

```sql
CREATE TABLE IF NOT EXISTS payments (
  payment_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 4: Create the DLQ events migration**

`db/migrations/002_create_dlq_events.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS dlq_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  event_key TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  failure_reason TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  reprocessed_at TIMESTAMPTZ
);
```

- [ ] **Step 5: Create the reconciliation cursor migration**

`db/migrations/003_create_reconciliation_cursor.sql`:

```sql
CREATE TABLE IF NOT EXISTS reconciliation_cursor (
  id TEXT PRIMARY KEY,
  cursor TEXT,
  updated_at TIMESTAMPTZ NOT NULL
);
```

- [ ] **Step 6: Create the migration runner**

`db/migrate.ts`:

```ts
import { readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

async function migrate(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion';
  const pool = new Pool({ connectionString: databaseUrl });

  await pool.query(`
    CREATE TABLE IF NOT EXISTS schema_migrations (
      filename TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  const migrationsDir = path.join(__dirname, 'migrations');
  const files = readdirSync(migrationsDir)
    .filter((file) => file.endsWith('.sql'))
    .sort();

  for (const file of files) {
    const alreadyApplied = await pool.query('SELECT 1 FROM schema_migrations WHERE filename = $1', [file]);
    if (alreadyApplied.rows.length > 0) {
      continue;
    }

    const sql = readFileSync(path.join(migrationsDir, file), 'utf-8');
    await pool.query(sql);
    await pool.query('INSERT INTO schema_migrations (filename) VALUES ($1)', [file]);
    console.log(`applied migration ${file}`);
  }

  await pool.end();
}

migrate().catch((error) => {
  console.error('migration failed', error);
  process.exit(1);
});
```

- [ ] **Step 7: Run the migrations and verify the tables exist**

Run: `npm run migrate`
Expected: logs `applied migration 001_create_payments.sql`, `applied migration 002_create_dlq_events.sql`, `applied migration 003_create_reconciliation_cursor.sql`.

Run: `npm run migrate` again
Expected: exits 0 with no "applied migration" lines (idempotent — already applied).

Run:

```bash
docker compose exec -T postgres psql -U postgres -d webhook_ingestion -c "\dt"
```

Expected: lists `payments`, `dlq_events`, `reconciliation_cursor`, and `schema_migrations`.

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml db/
git commit -m "chore: add local infrastructure and database migrations"
```

---

### Task 3: `PaymentEventValidator` (TDD)

**Files:**
- Create: `src/domain/services/PaymentEventValidator.ts`
- Test: `tests/unit/domain/PaymentEventValidator.test.ts`

**Interfaces:**
- Consumes: `PaymentEvent`, `PaymentEventType` from `src/domain/value-objects/PaymentEvent.ts` (Task 1)
- Produces:
  - `InvalidPaymentEventError extends Error`
  - `validatePaymentEvent(raw: unknown): PaymentEvent` — throws `InvalidPaymentEventError` on an invalid shape

- [ ] **Step 1: Write the failing test**

`tests/unit/domain/PaymentEventValidator.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { validatePaymentEvent, InvalidPaymentEventError } from '../../../src/domain/services/PaymentEventValidator';

describe('validatePaymentEvent', () => {
  it('parses a valid payment.succeeded event', () => {
    const event = validatePaymentEvent({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(event).toEqual({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });
  });

  it('parses a valid payment.failed event', () => {
    const event = validatePaymentEvent({ eventId: 'evt_2', paymentId: 'pay_2', type: 'payment.failed' });

    expect(event).toEqual({ eventId: 'evt_2', paymentId: 'pay_2', type: 'payment.failed' });
  });

  it('throws InvalidPaymentEventError when paymentId is missing', () => {
    expect(() => validatePaymentEvent({ eventId: 'evt_1', type: 'payment.succeeded' })).toThrow(
      InvalidPaymentEventError,
    );
  });

  it('throws InvalidPaymentEventError when type is not a known value', () => {
    expect(() =>
      validatePaymentEvent({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.pending' }),
    ).toThrow(InvalidPaymentEventError);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/domain/PaymentEventValidator.test.ts`
Expected: FAIL — cannot find module `../../../src/domain/services/PaymentEventValidator`.

- [ ] **Step 3: Implement `PaymentEventValidator`**

`src/domain/services/PaymentEventValidator.ts`:

```ts
import { z } from 'zod';
import { PaymentEvent } from '../value-objects/PaymentEvent';

const paymentEventSchema = z.object({
  eventId: z.string().min(1),
  paymentId: z.string().min(1),
  type: z.enum(['payment.succeeded', 'payment.failed']),
});

export class InvalidPaymentEventError extends Error {}

export function validatePaymentEvent(raw: unknown): PaymentEvent {
  const result = paymentEventSchema.safeParse(raw);
  if (!result.success) {
    throw new InvalidPaymentEventError(result.error.message);
  }
  return result.data;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/domain/PaymentEventValidator.test.ts`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/domain/services/ tests/unit/domain/
git commit -m "feat: add PaymentEventValidator"
```

---

### Task 4: Application ports

**Files:**
- Create: `src/application/ports/EventPublisherPort.ts`
- Create: `src/application/ports/PaymentRepositoryPort.ts`
- Create: `src/application/ports/DlqRepositoryPort.ts`
- Create: `src/application/ports/DedupeStorePort.ts`
- Create: `src/application/ports/PartnerEventsApiPort.ts`
- Create: `src/application/ports/ReconciliationCursorPort.ts`

**Interfaces:**
- Consumes: `PaymentEvent`, `PaymentEventType` from `src/domain/value-objects/PaymentEvent.ts` (Task 1)
- Produces:
  - `EventPublisherPort { publish(event: PaymentEvent): Promise<void> }`
  - `PaymentRepositoryPort { upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void> }`
  - `DlqEntry { id: string; eventKey: string; eventType: string; payload: unknown; failureReason: string; attempts: number; createdAt: Date; reprocessedAt: Date | null }`
  - `DlqRepositoryPort { add(entry: { eventKey: string; eventType: string; payload: unknown; failureReason: string; attempts: number }): Promise<string>; list(options: { limit: number; offset: number }): Promise<DlqEntry[]>; get(id: string): Promise<DlqEntry | null>; markReprocessed(id: string): Promise<void> }`
  - `DedupeStorePort { wasProcessed(eventId: string): Promise<boolean>; markProcessed(eventId: string): Promise<void> }`
  - `PartnerEventsPage { events: PaymentEvent[]; nextCursor: string | null }`
  - `PartnerEventsApiPort { listEvents(cursor: string | null, limit: number): Promise<PartnerEventsPage> }`
  - `ReconciliationCursorState { cursor: string | null; updatedAt: Date }`
  - `ReconciliationCursorPort { load(): Promise<ReconciliationCursorState | null>; save(cursor: string | null): Promise<void> }`

No behavior to test (pure types); verified via type-check and lint.

- [ ] **Step 1: Create `EventPublisherPort`**

`src/application/ports/EventPublisherPort.ts`:

```ts
import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';

export interface EventPublisherPort {
  publish(event: PaymentEvent): Promise<void>;
}
```

- [ ] **Step 2: Create `PaymentRepositoryPort`**

`src/application/ports/PaymentRepositoryPort.ts`:

```ts
import { PaymentEventType } from '../../domain/value-objects/PaymentEvent';

export interface PaymentRepositoryPort {
  upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void>;
}
```

- [ ] **Step 3: Create `DlqRepositoryPort`**

`src/application/ports/DlqRepositoryPort.ts`:

```ts
export interface DlqEntry {
  id: string;
  eventKey: string;
  eventType: string;
  payload: unknown;
  failureReason: string;
  attempts: number;
  createdAt: Date;
  reprocessedAt: Date | null;
}

export interface DlqRepositoryPort {
  add(entry: {
    eventKey: string;
    eventType: string;
    payload: unknown;
    failureReason: string;
    attempts: number;
  }): Promise<string>;
  list(options: { limit: number; offset: number }): Promise<DlqEntry[]>;
  get(id: string): Promise<DlqEntry | null>;
  markReprocessed(id: string): Promise<void>;
}
```

- [ ] **Step 4: Create `DedupeStorePort`**

`src/application/ports/DedupeStorePort.ts`:

```ts
export interface DedupeStorePort {
  wasProcessed(eventId: string): Promise<boolean>;
  markProcessed(eventId: string): Promise<void>;
}
```

- [ ] **Step 5: Create `PartnerEventsApiPort`**

`src/application/ports/PartnerEventsApiPort.ts`:

```ts
import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';

export interface PartnerEventsPage {
  events: PaymentEvent[];
  nextCursor: string | null;
}

export interface PartnerEventsApiPort {
  listEvents(cursor: string | null, limit: number): Promise<PartnerEventsPage>;
}
```

- [ ] **Step 6: Create `ReconciliationCursorPort`**

`src/application/ports/ReconciliationCursorPort.ts`:

```ts
export interface ReconciliationCursorState {
  cursor: string | null;
  updatedAt: Date;
}

export interface ReconciliationCursorPort {
  load(): Promise<ReconciliationCursorState | null>;
  save(cursor: string | null): Promise<void>;
}
```

- [ ] **Step 7: Verify tooling**

Run: `npm run typecheck && npm run lint`
Expected: both exit 0.

- [ ] **Step 8: Commit**

```bash
git add src/application/ports/
git commit -m "feat: add application ports"
```

---

### Task 5: Shared test fakes

**Files:**
- Create: `tests/fakes/FakeEventPublisherPort.ts`
- Create: `tests/fakes/FakePaymentRepositoryPort.ts`
- Create: `tests/fakes/FakeDlqRepositoryPort.ts`
- Create: `tests/fakes/FakeDedupeStorePort.ts`
- Create: `tests/fakes/FakePartnerEventsApiPort.ts`
- Create: `tests/fakes/FakeReconciliationCursorPort.ts`

**Interfaces:**
- Consumes: all 6 ports from Task 4; `PaymentEvent`, `PaymentEventType` from Task 1
- Produces:
  - `FakeEventPublisherPort` — public `published: PaymentEvent[]`
  - `FakePaymentRepositoryPort` — public `upserts: { paymentId: string; status: PaymentEventType }[]`
  - `FakeDlqRepositoryPort` — public `entries: DlqEntry[]`
  - `FakeDedupeStorePort` — no public state (query via its own methods)
  - `FakePartnerEventsApiPort` — constructor `(pages: Record<string, PartnerEventsPage>)`, where the key `'start'` represents a `null` cursor
  - `FakeReconciliationCursorPort` — constructor `(initial?: ReconciliationCursorState | null)`

These are test infrastructure with no assertions of their own; used starting in Task 6.

- [ ] **Step 1: Create `FakeEventPublisherPort`**

`tests/fakes/FakeEventPublisherPort.ts`:

```ts
import { EventPublisherPort } from '../../src/application/ports/EventPublisherPort';
import { PaymentEvent } from '../../src/domain/value-objects/PaymentEvent';

export class FakeEventPublisherPort implements EventPublisherPort {
  public published: PaymentEvent[] = [];

  async publish(event: PaymentEvent): Promise<void> {
    this.published.push(event);
  }
}
```

- [ ] **Step 2: Create `FakePaymentRepositoryPort`**

`tests/fakes/FakePaymentRepositoryPort.ts`:

```ts
import { PaymentRepositoryPort } from '../../src/application/ports/PaymentRepositoryPort';
import { PaymentEventType } from '../../src/domain/value-objects/PaymentEvent';

export class FakePaymentRepositoryPort implements PaymentRepositoryPort {
  public upserts: { paymentId: string; status: PaymentEventType }[] = [];

  async upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void> {
    this.upserts.push({ paymentId, status });
  }
}
```

- [ ] **Step 3: Create `FakeDlqRepositoryPort`**

`tests/fakes/FakeDlqRepositoryPort.ts`:

```ts
import { randomUUID } from 'node:crypto';
import { DlqEntry, DlqRepositoryPort } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepositoryPort implements DlqRepositoryPort {
  public entries: DlqEntry[] = [];

  async add(entry: {
    eventKey: string;
    eventType: string;
    payload: unknown;
    failureReason: string;
    attempts: number;
  }): Promise<string> {
    const id = randomUUID();
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    return this.entries
      .filter((entry) => entry.reprocessedAt === null)
      .slice(options.offset, options.offset + options.limit);
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
```

- [ ] **Step 4: Create `FakeDedupeStorePort`**

`tests/fakes/FakeDedupeStorePort.ts`:

```ts
import { DedupeStorePort } from '../../src/application/ports/DedupeStorePort';

export class FakeDedupeStorePort implements DedupeStorePort {
  private readonly processed = new Set<string>();

  async wasProcessed(eventId: string): Promise<boolean> {
    return this.processed.has(eventId);
  }

  async markProcessed(eventId: string): Promise<void> {
    this.processed.add(eventId);
  }
}
```

- [ ] **Step 5: Create `FakePartnerEventsApiPort`**

`tests/fakes/FakePartnerEventsApiPort.ts`:

```ts
import { PartnerEventsApiPort, PartnerEventsPage } from '../../src/application/ports/PartnerEventsApiPort';

export class FakePartnerEventsApiPort implements PartnerEventsApiPort {
  constructor(private readonly pages: Record<string, PartnerEventsPage>) {}

  async listEvents(cursor: string | null, _limit: number): Promise<PartnerEventsPage> {
    const key = cursor ?? 'start';
    const page = this.pages[key];
    if (!page) {
      throw new Error(`FakePartnerEventsApiPort: no page configured for cursor "${key}"`);
    }
    return page;
  }
}
```

- [ ] **Step 6: Create `FakeReconciliationCursorPort`**

`tests/fakes/FakeReconciliationCursorPort.ts`:

```ts
import {
  ReconciliationCursorPort,
  ReconciliationCursorState,
} from '../../src/application/ports/ReconciliationCursorPort';

export class FakeReconciliationCursorPort implements ReconciliationCursorPort {
  private state: ReconciliationCursorState | null;

  constructor(initial: ReconciliationCursorState | null = null) {
    this.state = initial;
  }

  async load(): Promise<ReconciliationCursorState | null> {
    return this.state;
  }

  async save(cursor: string | null): Promise<void> {
    this.state = { cursor, updatedAt: new Date() };
  }
}
```

- [ ] **Step 7: Verify tooling**

Run: `npm run typecheck && npm run lint`
Expected: both exit 0.

- [ ] **Step 8: Commit**

```bash
git add tests/fakes/
git commit -m "test: add shared port fakes"
```

---

### Task 6: `IngestWebhookEvent` use case (TDD)

**Files:**
- Create: `src/application/use-cases/IngestWebhookEvent.ts`
- Test: `tests/unit/application/IngestWebhookEvent.test.ts`

**Interfaces:**
- Consumes: `validatePaymentEvent`, `InvalidPaymentEventError` (Task 3); `EventPublisherPort` (Task 4); `FakeEventPublisherPort` (Task 5)
- Produces: `IngestWebhookEvent` class, constructor `(eventPublisher: EventPublisherPort)`, method `execute(rawPayload: unknown): Promise<void>`

- [ ] **Step 1: Write the failing test**

`tests/unit/application/IngestWebhookEvent.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { IngestWebhookEvent } from '../../../src/application/use-cases/IngestWebhookEvent';
import { InvalidPaymentEventError } from '../../../src/domain/services/PaymentEventValidator';
import { FakeEventPublisherPort } from '../../fakes/FakeEventPublisherPort';

describe('IngestWebhookEvent', () => {
  it('publishes a valid event', async () => {
    const publisher = new FakeEventPublisherPort();
    const useCase = new IngestWebhookEvent(publisher);

    await useCase.execute({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(publisher.published).toEqual([{ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' }]);
  });

  it('throws InvalidPaymentEventError and does not publish an invalid event', async () => {
    const publisher = new FakeEventPublisherPort();
    const useCase = new IngestWebhookEvent(publisher);

    await expect(useCase.execute({ paymentId: 'pay_1' })).rejects.toThrow(InvalidPaymentEventError);
    expect(publisher.published).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/application/IngestWebhookEvent.test.ts`
Expected: FAIL — cannot find module `../../../src/application/use-cases/IngestWebhookEvent`.

- [ ] **Step 3: Implement `IngestWebhookEvent`**

`src/application/use-cases/IngestWebhookEvent.ts`:

```ts
import { validatePaymentEvent } from '../../domain/services/PaymentEventValidator';
import { EventPublisherPort } from '../ports/EventPublisherPort';

export class IngestWebhookEvent {
  constructor(private readonly eventPublisher: EventPublisherPort) {}

  async execute(rawPayload: unknown): Promise<void> {
    const event = validatePaymentEvent(rawPayload);
    await this.eventPublisher.publish(event);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/application/IngestWebhookEvent.test.ts`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/application/use-cases/IngestWebhookEvent.ts tests/unit/application/IngestWebhookEvent.test.ts
git commit -m "feat: add IngestWebhookEvent use case"
```

---

### Task 7: `ProcessPaymentEvent` use case (TDD)

**Files:**
- Create: `src/application/use-cases/ProcessPaymentEvent.ts`
- Test: `tests/unit/application/ProcessPaymentEvent.test.ts`

**Interfaces:**
- Consumes: `validatePaymentEvent` (Task 3); `PaymentEvent` (Task 1); `PaymentRepositoryPort`, `DlqRepositoryPort`, `DedupeStorePort` (Task 4); fakes for all three (Task 5)
- Produces:
  - `ProcessPaymentEventOptions { maxAttempts?: number; backoffMs?: (attempt: number) => number; failOnPaymentId?: string }`
  - `ProcessPaymentEvent` class, constructor `(paymentRepository: PaymentRepositoryPort, dlqRepository: DlqRepositoryPort, dedupeStore: DedupeStorePort, options?: ProcessPaymentEventOptions)`, method `execute(rawPayload: unknown): Promise<void>`

This is the core delivery-guarantee logic: dedupe-after-success ordering, retry-then-DLQ, and the magic failure trigger (`pay_fail_demo` by default) all live here.

- [ ] **Step 1: Write the failing test**

`tests/unit/application/ProcessPaymentEvent.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { ProcessPaymentEvent } from '../../../src/application/use-cases/ProcessPaymentEvent';
import { FakePaymentRepositoryPort } from '../../fakes/FakePaymentRepositoryPort';
import { FakeDlqRepositoryPort } from '../../fakes/FakeDlqRepositoryPort';
import { FakeDedupeStorePort } from '../../fakes/FakeDedupeStorePort';

const noBackoff = (): number => 0;

describe('ProcessPaymentEvent', () => {
  it('skips processing when the event was already processed', async () => {
    const paymentRepository = new FakePaymentRepositoryPort();
    const dlqRepository = new FakeDlqRepositoryPort();
    const dedupeStore = new FakeDedupeStorePort();
    await dedupeStore.markProcessed('evt_1');
    const useCase = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
      backoffMs: noBackoff,
    });

    await useCase.execute({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(paymentRepository.upserts).toEqual([]);
    expect(dlqRepository.entries).toEqual([]);
  });

  it('upserts the payment and marks the event processed on success', async () => {
    const paymentRepository = new FakePaymentRepositoryPort();
    const dlqRepository = new FakeDlqRepositoryPort();
    const dedupeStore = new FakeDedupeStorePort();
    const useCase = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
      backoffMs: noBackoff,
    });

    await useCase.execute({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(paymentRepository.upserts).toEqual([{ paymentId: 'pay_1', status: 'payment.succeeded' }]);
    expect(await dedupeStore.wasProcessed('evt_1')).toBe(true);
    expect(dlqRepository.entries).toEqual([]);
  });

  it('sends the magic failure trigger to the DLQ after exhausting retries', async () => {
    const paymentRepository = new FakePaymentRepositoryPort();
    const dlqRepository = new FakeDlqRepositoryPort();
    const dedupeStore = new FakeDedupeStorePort();
    const useCase = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
      maxAttempts: 2,
      backoffMs: noBackoff,
    });

    await useCase.execute({ eventId: 'evt_fail', paymentId: 'pay_fail_demo', type: 'payment.succeeded' });

    expect(paymentRepository.upserts).toEqual([]);
    expect(await dedupeStore.wasProcessed('evt_fail')).toBe(false);
    expect(dlqRepository.entries).toHaveLength(1);
    expect(dlqRepository.entries[0].eventKey).toBe('evt_fail');
    expect(dlqRepository.entries[0].attempts).toBe(2);
    expect(dlqRepository.entries[0].failureReason).toContain('simulated processing failure');
  });

  it('sends a malformed payload to the DLQ, keyed by eventId when present', async () => {
    const paymentRepository = new FakePaymentRepositoryPort();
    const dlqRepository = new FakeDlqRepositoryPort();
    const dedupeStore = new FakeDedupeStorePort();
    const useCase = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
      maxAttempts: 1,
      backoffMs: noBackoff,
    });

    await useCase.execute({ eventId: 'evt_bad', type: 'payment.succeeded' });

    expect(paymentRepository.upserts).toEqual([]);
    expect(dlqRepository.entries).toHaveLength(1);
    expect(dlqRepository.entries[0].eventKey).toBe('evt_bad');
    expect(dlqRepository.entries[0].eventType).toBe('payment.succeeded');
  });

  it('does not mark the event processed when it lands in the DLQ', async () => {
    const paymentRepository = new FakePaymentRepositoryPort();
    const dlqRepository = new FakeDlqRepositoryPort();
    const dedupeStore = new FakeDedupeStorePort();
    const useCase = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
      maxAttempts: 1,
      backoffMs: noBackoff,
    });

    await useCase.execute({ eventId: 'evt_fail', paymentId: 'pay_fail_demo', type: 'payment.succeeded' });

    expect(await dedupeStore.wasProcessed('evt_fail')).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/application/ProcessPaymentEvent.test.ts`
Expected: FAIL — cannot find module `../../../src/application/use-cases/ProcessPaymentEvent`.

- [ ] **Step 3: Implement `ProcessPaymentEvent`**

`src/application/use-cases/ProcessPaymentEvent.ts`:

```ts
import { validatePaymentEvent } from '../../domain/services/PaymentEventValidator';
import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';
import { PaymentRepositoryPort } from '../ports/PaymentRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { DedupeStorePort } from '../ports/DedupeStorePort';

export interface ProcessPaymentEventOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
  failOnPaymentId?: string;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class ProcessPaymentEvent {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;
  private readonly failOnPaymentId: string;

  constructor(
    private readonly paymentRepository: PaymentRepositoryPort,
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly dedupeStore: DedupeStorePort,
    options: ProcessPaymentEventOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
    this.failOnPaymentId = options.failOnPaymentId ?? 'pay_fail_demo';
  }

  async execute(rawPayload: unknown): Promise<void> {
    const dedupeKey = this.extractEventId(rawPayload);

    if (dedupeKey !== null) {
      const alreadyProcessed = await this.dedupeStore.wasProcessed(dedupeKey);
      if (alreadyProcessed) {
        return;
      }
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validatePaymentEvent(rawPayload);
        this.assertShouldSucceed(event);
        await this.paymentRepository.upsertPaymentStatus(event.paymentId, event.type);
        await this.dedupeStore.markProcessed(event.eventId);
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      eventKey: dedupeKey ?? 'unknown',
      eventType: this.extractEventType(rawPayload),
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private assertShouldSucceed(event: PaymentEvent): void {
    if (event.paymentId === this.failOnPaymentId) {
      throw new Error(`simulated processing failure for paymentId "${event.paymentId}"`);
    }
  }

  private extractEventId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'eventId' in rawPayload) {
      const value = (rawPayload as { eventId: unknown }).eventId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private extractEventType(rawPayload: unknown): string {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'type' in rawPayload) {
      const value = (rawPayload as { type: unknown }).type;
      return typeof value === 'string' ? value : 'unknown';
    }
    return 'unknown';
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/application/ProcessPaymentEvent.test.ts`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add src/application/use-cases/ProcessPaymentEvent.ts tests/unit/application/ProcessPaymentEvent.test.ts
git commit -m "feat: add ProcessPaymentEvent use case with dedupe and retry-then-DLQ"
```

---

### Task 8: `ReprocessDlqEvent` use case (TDD)

**Files:**
- Create: `src/application/use-cases/ReprocessDlqEvent.ts`
- Test: `tests/unit/application/ReprocessDlqEvent.test.ts`

**Interfaces:**
- Consumes: `DlqRepositoryPort` (Task 4); `EventPublisherPort` (Task 4); `PaymentEvent` (Task 1); fakes (Task 5)
- Produces:
  - `DlqEntryNotFoundError extends Error`
  - `ReprocessDlqEvent` class, constructor `(dlqRepository: DlqRepositoryPort, eventPublisher: EventPublisherPort)`, method `execute(id: string): Promise<void>`

- [ ] **Step 1: Write the failing test**

`tests/unit/application/ReprocessDlqEvent.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { ReprocessDlqEvent, DlqEntryNotFoundError } from '../../../src/application/use-cases/ReprocessDlqEvent';
import { FakeDlqRepositoryPort } from '../../fakes/FakeDlqRepositoryPort';
import { FakeEventPublisherPort } from '../../fakes/FakeEventPublisherPort';

describe('ReprocessDlqEvent', () => {
  it('republishes the stored payload and marks the entry reprocessed', async () => {
    const dlqRepository = new FakeDlqRepositoryPort();
    const eventPublisher = new FakeEventPublisherPort();
    const payload = { eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' as const };
    const id = await dlqRepository.add({
      eventKey: 'evt_1',
      eventType: 'payment.succeeded',
      payload,
      failureReason: 'simulated processing failure',
      attempts: 3,
    });
    const useCase = new ReprocessDlqEvent(dlqRepository, eventPublisher);

    await useCase.execute(id);

    expect(eventPublisher.published).toEqual([payload]);
    const entry = await dlqRepository.get(id);
    expect(entry?.reprocessedAt).not.toBeNull();
  });

  it('throws DlqEntryNotFoundError for an unknown id', async () => {
    const dlqRepository = new FakeDlqRepositoryPort();
    const eventPublisher = new FakeEventPublisherPort();
    const useCase = new ReprocessDlqEvent(dlqRepository, eventPublisher);

    await expect(useCase.execute('missing-id')).rejects.toThrow(DlqEntryNotFoundError);
    expect(eventPublisher.published).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/application/ReprocessDlqEvent.test.ts`
Expected: FAIL — cannot find module `../../../src/application/use-cases/ReprocessDlqEvent`.

- [ ] **Step 3: Implement `ReprocessDlqEvent`**

`src/application/use-cases/ReprocessDlqEvent.ts`:

```ts
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { EventPublisherPort } from '../ports/EventPublisherPort';
import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';

export class DlqEntryNotFoundError extends Error {}

export class ReprocessDlqEvent {
  constructor(
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly eventPublisher: EventPublisherPort,
  ) {}

  async execute(id: string): Promise<void> {
    const entry = await this.dlqRepository.get(id);
    if (!entry) {
      throw new DlqEntryNotFoundError(`no DLQ entry found with id "${id}"`);
    }

    // The stored payload's runtime shape is whatever caused the original
    // failure (it may itself be malformed) — republishing it unchanged lets
    // it flow through the same validation ProcessPaymentEvent already does.
    await this.eventPublisher.publish(entry.payload as PaymentEvent);
    await this.dlqRepository.markReprocessed(id);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/application/ReprocessDlqEvent.test.ts`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/application/use-cases/ReprocessDlqEvent.ts tests/unit/application/ReprocessDlqEvent.test.ts
git commit -m "feat: add ReprocessDlqEvent use case"
```

---

### Task 9: `ReconcilePayments` use case (TDD)

**Files:**
- Create: `src/application/use-cases/ReconcilePayments.ts`
- Test: `tests/unit/application/ReconcilePayments.test.ts`

**Interfaces:**
- Consumes: `PartnerEventsApiPort`, `EventPublisherPort`, `ReconciliationCursorPort` (Task 4); fakes (Task 5)
- Produces:
  - `ReconcilePaymentsOptions { pageSize?: number; staleAfterMs?: number; now?: () => Date }`
  - `ReconcilePayments` class, constructor `(partnerEventsApi: PartnerEventsApiPort, eventPublisher: EventPublisherPort, cursorRepository: ReconciliationCursorPort, options?: ReconcilePaymentsOptions)`, method `execute(): Promise<void>`

- [ ] **Step 1: Write the failing test**

`tests/unit/application/ReconcilePayments.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { ReconcilePayments } from '../../../src/application/use-cases/ReconcilePayments';
import { FakePartnerEventsApiPort } from '../../fakes/FakePartnerEventsApiPort';
import { FakeEventPublisherPort } from '../../fakes/FakeEventPublisherPort';
import { FakeReconciliationCursorPort } from '../../fakes/FakeReconciliationCursorPort';

const event1 = { eventId: 'partner_evt_1', paymentId: 'partner_pay_1', type: 'payment.succeeded' as const };
const event2 = { eventId: 'partner_evt_2', paymentId: 'partner_pay_2', type: 'payment.succeeded' as const };

describe('ReconcilePayments', () => {
  it('performs a full resync from the start when no cursor was saved', async () => {
    const partnerEventsApi = new FakePartnerEventsApiPort({
      start: { events: [event1], nextCursor: 'c1' },
    });
    const eventPublisher = new FakeEventPublisherPort();
    const cursorRepository = new FakeReconciliationCursorPort(null);
    const useCase = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository);

    await useCase.execute();

    expect(eventPublisher.published).toEqual([event1]);
    expect((await cursorRepository.load())?.cursor).toBe('c1');
  });

  it('performs an incremental sync from the saved cursor when it is fresh', async () => {
    const partnerEventsApi = new FakePartnerEventsApiPort({
      c1: { events: [event2], nextCursor: 'c2' },
    });
    const eventPublisher = new FakeEventPublisherPort();
    const cursorRepository = new FakeReconciliationCursorPort({ cursor: 'c1', updatedAt: new Date() });
    const useCase = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository);

    await useCase.execute();

    expect(eventPublisher.published).toEqual([event2]);
    expect((await cursorRepository.load())?.cursor).toBe('c2');
  });

  it('treats a stale saved cursor as a full resync', async () => {
    const twentyFiveHoursAgo = new Date(Date.now() - 25 * 60 * 60 * 1000);
    const partnerEventsApi = new FakePartnerEventsApiPort({
      start: { events: [event1], nextCursor: 'c1' },
    });
    const eventPublisher = new FakeEventPublisherPort();
    const cursorRepository = new FakeReconciliationCursorPort({ cursor: 'stale_cursor', updatedAt: twentyFiveHoursAgo });
    const useCase = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository);

    await useCase.execute();

    expect(eventPublisher.published).toEqual([event1]);
  });

  it('walks multiple pages while a page is full, and saves the last seen cursor', async () => {
    const partnerEventsApi = new FakePartnerEventsApiPort({
      start: { events: [event1], nextCursor: 'c1' },
      c1: { events: [event2], nextCursor: 'c2' },
      c2: { events: [], nextCursor: null },
    });
    const eventPublisher = new FakeEventPublisherPort();
    const cursorRepository = new FakeReconciliationCursorPort(null);
    const useCase = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository, { pageSize: 1 });

    await useCase.execute();

    expect(eventPublisher.published).toEqual([event1, event2]);
    expect((await cursorRepository.load())?.cursor).toBe('c2');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/application/ReconcilePayments.test.ts`
Expected: FAIL — cannot find module `../../../src/application/use-cases/ReconcilePayments`.

- [ ] **Step 3: Implement `ReconcilePayments`**

`src/application/use-cases/ReconcilePayments.ts`:

```ts
import { PartnerEventsApiPort } from '../ports/PartnerEventsApiPort';
import { EventPublisherPort } from '../ports/EventPublisherPort';
import { ReconciliationCursorPort } from '../ports/ReconciliationCursorPort';

export interface ReconcilePaymentsOptions {
  pageSize?: number;
  staleAfterMs?: number;
  now?: () => Date;
}

const DEFAULT_STALE_AFTER_MS = 24 * 60 * 60 * 1000;

export class ReconcilePayments {
  private readonly pageSize: number;
  private readonly staleAfterMs: number;
  private readonly now: () => Date;

  constructor(
    private readonly partnerEventsApi: PartnerEventsApiPort,
    private readonly eventPublisher: EventPublisherPort,
    private readonly cursorRepository: ReconciliationCursorPort,
    options: ReconcilePaymentsOptions = {},
  ) {
    this.pageSize = options.pageSize ?? 50;
    this.staleAfterMs = options.staleAfterMs ?? DEFAULT_STALE_AFTER_MS;
    this.now = options.now ?? ((): Date => new Date());
  }

  async execute(): Promise<void> {
    const saved = await this.cursorRepository.load();
    const isStale = saved !== null && this.now().getTime() - saved.updatedAt.getTime() > this.staleAfterMs;
    let cursor: string | null = saved === null || isStale ? null : saved.cursor;
    let lastCursor: string | null = cursor;

    for (;;) {
      const page = await this.partnerEventsApi.listEvents(cursor, this.pageSize);

      for (const event of page.events) {
        await this.eventPublisher.publish(event);
      }

      if (page.nextCursor !== null) {
        lastCursor = page.nextCursor;
      }

      if (page.events.length < this.pageSize) {
        break;
      }

      cursor = page.nextCursor;
    }

    await this.cursorRepository.save(lastCursor);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/application/ReconcilePayments.test.ts`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/application/use-cases/ReconcilePayments.ts tests/unit/application/ReconcilePayments.test.ts
git commit -m "feat: add ReconcilePayments use case with cursor-based sync"
```

---

### Task 10: `loadEnv` config (TDD)

**Files:**
- Create: `src/config/env.ts`
- Test: `tests/unit/config/env.test.ts`

**Interfaces:**
- Produces:
  - `AppEnv { port: number; databaseUrl: string; redisUrl: string; kafkaBrokers: string[]; partnerApiBaseUrl: string; reconciliationIntervalMs: number }`
  - `loadEnv(): AppEnv`

- [ ] **Step 1: Write the failing test**

`tests/unit/config/env.test.ts`:

```ts
import { describe, it, expect, afterEach } from 'vitest';
import { loadEnv } from '../../../src/config/env';

const originalEnv = { ...process.env };

afterEach(() => {
  process.env = { ...originalEnv };
});

describe('loadEnv', () => {
  it('returns sensible defaults when nothing is set', () => {
    delete process.env.PORT;
    delete process.env.DATABASE_URL;
    delete process.env.REDIS_URL;
    delete process.env.KAFKA_BROKERS;
    delete process.env.PARTNER_API_BASE_URL;
    delete process.env.RECONCILIATION_INTERVAL_MS;

    const env = loadEnv();

    expect(env).toEqual({
      port: 3000,
      databaseUrl: 'postgres://postgres:postgres@localhost:5432/webhook_ingestion',
      redisUrl: 'redis://localhost:6379',
      kafkaBrokers: ['localhost:9092'],
      partnerApiBaseUrl: 'http://localhost:4002',
      reconciliationIntervalMs: 60 * 60 * 1000,
    });
  });

  it('reads every variable from the environment when set, splitting KAFKA_BROKERS on commas', () => {
    process.env.PORT = '5050';
    process.env.DATABASE_URL = 'postgres://u:p@db:5432/app';
    process.env.REDIS_URL = 'redis://cache:6379';
    process.env.KAFKA_BROKERS = 'broker1:9092,broker2:9092';
    process.env.PARTNER_API_BASE_URL = 'http://partner:4002';
    process.env.RECONCILIATION_INTERVAL_MS = '1000';

    const env = loadEnv();

    expect(env).toEqual({
      port: 5050,
      databaseUrl: 'postgres://u:p@db:5432/app',
      redisUrl: 'redis://cache:6379',
      kafkaBrokers: ['broker1:9092', 'broker2:9092'],
      partnerApiBaseUrl: 'http://partner:4002',
      reconciliationIntervalMs: 1000,
    });
  });

  it('throws when PORT is not a number', () => {
    process.env.PORT = 'abc';

    expect(() => loadEnv()).toThrow('Invalid PORT environment variable: "abc" is not a number');
  });

  it('throws when RECONCILIATION_INTERVAL_MS is not a number', () => {
    process.env.RECONCILIATION_INTERVAL_MS = 'abc';

    expect(() => loadEnv()).toThrow(
      'Invalid RECONCILIATION_INTERVAL_MS environment variable: "abc" is not a number',
    );
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/config/env.test.ts`
Expected: FAIL — cannot find module `../../../src/config/env`.

- [ ] **Step 3: Implement `loadEnv`**

`src/config/env.ts`:

```ts
export interface AppEnv {
  port: number;
  databaseUrl: string;
  redisUrl: string;
  kafkaBrokers: string[];
  partnerApiBaseUrl: string;
  reconciliationIntervalMs: number;
}

export function loadEnv(): AppEnv {
  const rawPort = process.env.PORT;
  const port = rawPort === undefined ? 3000 : Number(rawPort);
  if (Number.isNaN(port)) {
    throw new Error(`Invalid PORT environment variable: "${rawPort}" is not a number`);
  }

  const rawInterval = process.env.RECONCILIATION_INTERVAL_MS;
  const reconciliationIntervalMs = rawInterval === undefined ? 60 * 60 * 1000 : Number(rawInterval);
  if (Number.isNaN(reconciliationIntervalMs)) {
    throw new Error(`Invalid RECONCILIATION_INTERVAL_MS environment variable: "${rawInterval}" is not a number`);
  }

  return {
    port,
    databaseUrl: process.env.DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion',
    redisUrl: process.env.REDIS_URL ?? 'redis://localhost:6379',
    kafkaBrokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
    partnerApiBaseUrl: process.env.PARTNER_API_BASE_URL ?? 'http://localhost:4002',
    reconciliationIntervalMs,
  };
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run tests/unit/config/env.test.ts`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/config/ tests/unit/config/
git commit -m "feat: add environment configuration loader"
```

---

### Task 11: `RedisDedupeStore` adapter (TDD, real local Redis)

**Files:**
- Create: `src/adapters/outbound/redis/RedisDedupeStore.ts`
- Test: `tests/unit/adapters/RedisDedupeStore.test.ts`

**Interfaces:**
- Consumes: `DedupeStorePort` (Task 4); the real `redis` package's `createClient`
- Produces: `RedisDedupeStore` class, constructor `(client: ReturnType<typeof createClient>)`, implements `DedupeStorePort`

Requires the infrastructure from Task 2 running: `npm run infra:up` (or confirm `docker compose ps` shows `redis` healthy) before running this task's tests.

- [ ] **Step 1: Write the failing test**

`tests/unit/adapters/RedisDedupeStore.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createClient } from 'redis';
import { RedisDedupeStore } from '../../../src/adapters/outbound/redis/RedisDedupeStore';

const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379';

let client: ReturnType<typeof createClient>;
let store: RedisDedupeStore;

beforeAll(async () => {
  client = createClient({ url: REDIS_URL });
  await client.connect();
  store = new RedisDedupeStore(client);
});

afterAll(async () => {
  await client.quit();
});

beforeEach(async () => {
  await client.flushDb();
});

describe('RedisDedupeStore', () => {
  it('reports an eventId as not processed before it is marked', async () => {
    expect(await store.wasProcessed('evt_1')).toBe(false);
  });

  it('reports an eventId as processed after it is marked', async () => {
    await store.markProcessed('evt_1');

    expect(await store.wasProcessed('evt_1')).toBe(true);
  });

  it('keeps dedupe state independent per eventId', async () => {
    await store.markProcessed('evt_1');

    expect(await store.wasProcessed('evt_2')).toBe(false);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/adapters/RedisDedupeStore.test.ts`
Expected: FAIL — cannot find module `../../../src/adapters/outbound/redis/RedisDedupeStore`.

- [ ] **Step 3: Implement `RedisDedupeStore`**

`src/adapters/outbound/redis/RedisDedupeStore.ts`:

```ts
import { createClient } from 'redis';
import { DedupeStorePort } from '../../../application/ports/DedupeStorePort';

type RedisClient = ReturnType<typeof createClient>;

const DEDUPE_TTL_SECONDS = 24 * 60 * 60;

export class RedisDedupeStore implements DedupeStorePort {
  constructor(private readonly client: RedisClient) {}

  async wasProcessed(eventId: string): Promise<boolean> {
    const value = await this.client.get(this.key(eventId));
    return value !== null;
  }

  async markProcessed(eventId: string): Promise<void> {
    await this.client.set(this.key(eventId), '1', { EX: DEDUPE_TTL_SECONDS });
  }

  private key(eventId: string): string {
    return `dedupe:${eventId}`;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Confirm infrastructure is up: `docker compose ps` should show `redis` as healthy (if not, run `npm run infra:up` and wait).

Run: `npx vitest run tests/unit/adapters/RedisDedupeStore.test.ts`
Expected: PASS (3 tests), against the real local Redis.

- [ ] **Step 5: Commit**

```bash
git add src/adapters/outbound/redis/ tests/unit/adapters/RedisDedupeStore.test.ts
git commit -m "feat: add RedisDedupeStore adapter"
```

---

### Task 12: Postgres repositories (TDD, real local Postgres)

**Files:**
- Create: `src/adapters/outbound/postgres/PostgresPaymentRepository.ts`
- Create: `src/adapters/outbound/postgres/PostgresDlqRepository.ts`
- Create: `src/adapters/outbound/postgres/PostgresReconciliationCursorRepository.ts`
- Test: `tests/unit/adapters/PostgresRepositories.test.ts`

**Interfaces:**
- Consumes: `PaymentRepositoryPort`, `DlqRepositoryPort`, `DlqEntry`, `ReconciliationCursorPort`, `ReconciliationCursorState` (Task 4); `PaymentEventType` (Task 1); the `pg` package's `Pool`; the `payments`/`dlq_events`/`reconciliation_cursor` tables (Task 2)
- Produces:
  - `PostgresPaymentRepository` class, constructor `(pool: Pool)`, implements `PaymentRepositoryPort`
  - `PostgresDlqRepository` class, constructor `(pool: Pool)`, implements `DlqRepositoryPort`
  - `PostgresReconciliationCursorRepository` class, constructor `(pool: Pool)`, implements `ReconciliationCursorPort`

Requires the infrastructure and migrations from Task 2 (`npm run infra:up`, `npm run migrate`).

- [ ] **Step 1: Write the failing tests**

`tests/unit/adapters/PostgresRepositories.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { Pool } from 'pg';
import { PostgresPaymentRepository } from '../../../src/adapters/outbound/postgres/PostgresPaymentRepository';
import { PostgresDlqRepository } from '../../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { PostgresReconciliationCursorRepository } from '../../../src/adapters/outbound/postgres/PostgresReconciliationCursorRepository';

const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion';

let pool: Pool;

beforeAll(() => {
  pool = new Pool({ connectionString: DATABASE_URL });
});

afterAll(async () => {
  await pool.end();
});

beforeEach(async () => {
  await pool.query('TRUNCATE payments, dlq_events, reconciliation_cursor');
});

describe('PostgresPaymentRepository', () => {
  it('inserts a new payment status', async () => {
    const repo = new PostgresPaymentRepository(pool);

    await repo.upsertPaymentStatus('pay_1', 'payment.succeeded');

    const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_1']);
    expect(result.rows[0].status).toBe('payment.succeeded');
  });

  it('updates the status on a repeated upsert instead of duplicating the row', async () => {
    const repo = new PostgresPaymentRepository(pool);

    await repo.upsertPaymentStatus('pay_1', 'payment.succeeded');
    await repo.upsertPaymentStatus('pay_1', 'payment.failed');

    const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_1']);
    expect(result.rows).toHaveLength(1);
    expect(result.rows[0].status).toBe('payment.failed');
  });
});

describe('PostgresDlqRepository', () => {
  it('adds an entry and lists it as unresolved', async () => {
    const repo = new PostgresDlqRepository(pool);

    const id = await repo.add({
      eventKey: 'evt_1',
      eventType: 'payment.succeeded',
      payload: { eventId: 'evt_1' },
      failureReason: 'boom',
      attempts: 3,
    });

    const listed = await repo.list({ limit: 10, offset: 0 });
    expect(listed).toHaveLength(1);
    expect(listed[0].id).toBe(id);
    expect(listed[0].payload).toEqual({ eventId: 'evt_1' });
  });

  it('excludes reprocessed entries from the default listing', async () => {
    const repo = new PostgresDlqRepository(pool);
    const id = await repo.add({
      eventKey: 'evt_1',
      eventType: 'payment.succeeded',
      payload: { eventId: 'evt_1' },
      failureReason: 'boom',
      attempts: 3,
    });

    await repo.markReprocessed(id);

    const listed = await repo.list({ limit: 10, offset: 0 });
    expect(listed).toHaveLength(0);
    const entry = await repo.get(id);
    expect(entry?.reprocessedAt).not.toBeNull();
  });

  it('returns null from get for an unknown id', async () => {
    const repo = new PostgresDlqRepository(pool);

    expect(await repo.get('00000000-0000-0000-0000-000000000000')).toBeNull();
  });
});

describe('PostgresReconciliationCursorRepository', () => {
  it('returns null when no cursor has been saved yet', async () => {
    const repo = new PostgresReconciliationCursorRepository(pool);

    expect(await repo.load()).toBeNull();
  });

  it('saves and loads the cursor', async () => {
    const repo = new PostgresReconciliationCursorRepository(pool);

    await repo.save('cursor_123');
    const loaded = await repo.load();

    expect(loaded?.cursor).toBe('cursor_123');
  });

  it('overwrites the cursor on a repeated save', async () => {
    const repo = new PostgresReconciliationCursorRepository(pool);

    await repo.save('cursor_1');
    await repo.save('cursor_2');
    const loaded = await repo.load();

    expect(loaded?.cursor).toBe('cursor_2');
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/unit/adapters/PostgresRepositories.test.ts`
Expected: FAIL — cannot find the three repository modules.

- [ ] **Step 3: Implement `PostgresPaymentRepository`**

`src/adapters/outbound/postgres/PostgresPaymentRepository.ts`:

```ts
import { Pool } from 'pg';
import { PaymentRepositoryPort } from '../../../application/ports/PaymentRepositoryPort';
import { PaymentEventType } from '../../../domain/value-objects/PaymentEvent';

export class PostgresPaymentRepository implements PaymentRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void> {
    await this.pool.query(
      `INSERT INTO payments (payment_id, status, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (payment_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at`,
      [paymentId, status],
    );
  }
}
```

- [ ] **Step 4: Implement `PostgresDlqRepository`**

`src/adapters/outbound/postgres/PostgresDlqRepository.ts`:

```ts
import { Pool } from 'pg';
import { DlqEntry, DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';

interface DlqRow {
  id: string;
  event_key: string;
  event_type: string;
  payload: unknown;
  failure_reason: string;
  attempts: number;
  created_at: Date;
  reprocessed_at: Date | null;
}

function toDlqEntry(row: DlqRow): DlqEntry {
  return {
    id: row.id,
    eventKey: row.event_key,
    eventType: row.event_type,
    payload: row.payload,
    failureReason: row.failure_reason,
    attempts: row.attempts,
    createdAt: row.created_at,
    reprocessedAt: row.reprocessed_at,
  };
}

export class PostgresDlqRepository implements DlqRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async add(entry: {
    eventKey: string;
    eventType: string;
    payload: unknown;
    failureReason: string;
    attempts: number;
  }): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO dlq_events (event_key, event_type, payload, failure_reason, attempts)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id`,
      [entry.eventKey, entry.eventType, JSON.stringify(entry.payload), entry.failureReason, entry.attempts],
    );
    return result.rows[0].id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    const result = await this.pool.query<DlqRow>(
      `SELECT * FROM dlq_events WHERE reprocessed_at IS NULL ORDER BY created_at ASC LIMIT $1 OFFSET $2`,
      [options.limit, options.offset],
    );
    return result.rows.map(toDlqEntry);
  }

  async get(id: string): Promise<DlqEntry | null> {
    const result = await this.pool.query<DlqRow>(`SELECT * FROM dlq_events WHERE id = $1`, [id]);
    return result.rows[0] ? toDlqEntry(result.rows[0]) : null;
  }

  async markReprocessed(id: string): Promise<void> {
    await this.pool.query(`UPDATE dlq_events SET reprocessed_at = now() WHERE id = $1`, [id]);
  }
}
```

- [ ] **Step 5: Implement `PostgresReconciliationCursorRepository`**

`src/adapters/outbound/postgres/PostgresReconciliationCursorRepository.ts`:

```ts
import { Pool } from 'pg';
import {
  ReconciliationCursorPort,
  ReconciliationCursorState,
} from '../../../application/ports/ReconciliationCursorPort';

const CURSOR_ID = 'partner_events';

export class PostgresReconciliationCursorRepository implements ReconciliationCursorPort {
  constructor(private readonly pool: Pool) {}

  async load(): Promise<ReconciliationCursorState | null> {
    const result = await this.pool.query<{ cursor: string | null; updated_at: Date }>(
      `SELECT cursor, updated_at FROM reconciliation_cursor WHERE id = $1`,
      [CURSOR_ID],
    );
    if (result.rows.length === 0) {
      return null;
    }
    return { cursor: result.rows[0].cursor, updatedAt: result.rows[0].updated_at };
  }

  async save(cursor: string | null): Promise<void> {
    await this.pool.query(
      `INSERT INTO reconciliation_cursor (id, cursor, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at`,
      [CURSOR_ID, cursor],
    );
  }
}
```

- [ ] **Step 6: Run tests to verify they pass**

Confirm infrastructure and migrations are applied: `docker compose ps` shows `postgres` healthy; `npm run migrate` has been run at least once (Task 2).

Run: `npx vitest run tests/unit/adapters/PostgresRepositories.test.ts`
Expected: PASS (8 tests), against the real local Postgres.

- [ ] **Step 7: Commit**

```bash
git add src/adapters/outbound/postgres/ tests/unit/adapters/PostgresRepositories.test.ts
git commit -m "feat: add Postgres repositories"
```

---

### Task 13: `KafkaEventPublisher` adapter (TDD, real local Kafka)

**Files:**
- Create: `src/adapters/outbound/kafka/KafkaEventPublisher.ts`
- Test: `tests/unit/adapters/KafkaEventPublisher.test.ts`

**Interfaces:**
- Consumes: `EventPublisherPort` (Task 4); `PaymentEvent` (Task 1); the `kafkajs` package
- Produces:
  - `PAYMENT_EVENTS_TOPIC = 'payment-events'`
  - `createKafka(brokers: string[]): Kafka`
  - `KafkaEventPublisher` class, constructor `(producer: Producer)`, implements `EventPublisherPort`

Requires the Kafka infrastructure from Task 2 running and reachable at `localhost:9092`.

- [ ] **Step 1: Write the failing test**

`tests/unit/adapters/KafkaEventPublisher.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Kafka, Consumer, Producer } from 'kafkajs';
import {
  KafkaEventPublisher,
  createKafka,
  PAYMENT_EVENTS_TOPIC,
} from '../../../src/adapters/outbound/kafka/KafkaEventPublisher';

const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let kafka: Kafka;
let publisher: KafkaEventPublisher;
let producer: Producer;
let consumer: Consumer;

beforeAll(async () => {
  kafka = createKafka(KAFKA_BROKERS);

  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: PAYMENT_EVENTS_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  producer = kafka.producer();
  await producer.connect();
  publisher = new KafkaEventPublisher(producer);

  consumer = kafka.consumer({ groupId: `kafka-event-publisher-test-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: PAYMENT_EVENTS_TOPIC, fromBeginning: false });
}, 30000);

afterAll(async () => {
  await consumer.disconnect();
  await producer.disconnect();
}, 30000);

describe('KafkaEventPublisher', () => {
  it('publishes an event that a consumer can read back', async () => {
    const received = new Promise<string>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }) => {
          resolve(message.value?.toString() ?? '');
        },
      });
    });

    // give the consumer group a moment to finish joining before publishing
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await publisher.publish({ eventId: 'evt_test_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    const value = await received;
    expect(JSON.parse(value)).toEqual({ eventId: 'evt_test_1', paymentId: 'pay_1', type: 'payment.succeeded' });
  }, 30000);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run tests/unit/adapters/KafkaEventPublisher.test.ts`
Expected: FAIL — cannot find module `../../../src/adapters/outbound/kafka/KafkaEventPublisher`.

- [ ] **Step 3: Implement `KafkaEventPublisher`**

`src/adapters/outbound/kafka/KafkaEventPublisher.ts`:

```ts
import { Kafka, Producer } from 'kafkajs';
import { EventPublisherPort } from '../../../application/ports/EventPublisherPort';
import { PaymentEvent } from '../../../domain/value-objects/PaymentEvent';

export const PAYMENT_EVENTS_TOPIC = 'payment-events';

export function createKafka(brokers: string[]): Kafka {
  return new Kafka({ clientId: 'webhook-ingestion-replay-dlq', brokers });
}

export class KafkaEventPublisher implements EventPublisherPort {
  constructor(private readonly producer: Producer) {}

  async publish(event: PaymentEvent): Promise<void> {
    await this.producer.send({
      topic: PAYMENT_EVENTS_TOPIC,
      messages: [{ key: event.eventId, value: JSON.stringify(event) }],
    });
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Confirm Kafka is reachable: `docker compose ps` shows `kafka` running (from Task 2's verification).

Run: `npx vitest run tests/unit/adapters/KafkaEventPublisher.test.ts`
Expected: PASS (1 test), against the real local Kafka.

- [ ] **Step 5: Commit**

```bash
git add src/adapters/outbound/kafka/KafkaEventPublisher.ts tests/unit/adapters/KafkaEventPublisher.test.ts
git commit -m "feat: add KafkaEventPublisher adapter"
```

---

### Task 14: Mock partner API and `HttpPartnerEventsApiClient` (TDD)

**Files:**
- Create: `mock-partner-api/server.ts`
- Create: `src/adapters/outbound/partner-api/HttpPartnerEventsApiClient.ts`
- Test: `tests/unit/adapters/HttpPartnerEventsApiClient.test.ts`

**Interfaces:**
- Consumes: `PartnerEventsApiPort`, `PartnerEventsPage` (Task 4); `PaymentEvent` (Task 1)
- Produces:
  - `createMockPartnerApiApp(): Express` — serves `GET /events?cursor=&limit=`, a fixed seeded list of 120 synthetic events, cursor-paginated by integer offset
  - `HttpPartnerEventsApiClient` class, constructor `(baseUrl: string)`, implements `PartnerEventsApiPort`

- [ ] **Step 1: Implement the mock partner API**

`mock-partner-api/server.ts`:

```ts
import express, { Express } from 'express';

interface SyntheticEvent {
  eventId: string;
  paymentId: string;
  type: 'payment.succeeded' | 'payment.failed';
}

function generateEvents(count: number): SyntheticEvent[] {
  const events: SyntheticEvent[] = [];
  for (let i = 0; i < count; i += 1) {
    events.push({
      eventId: `partner_evt_${i}`,
      paymentId: `partner_pay_${i}`,
      type: i % 5 === 0 ? 'payment.failed' : 'payment.succeeded',
    });
  }
  return events;
}

const ALL_EVENTS = generateEvents(120);

export function createMockPartnerApiApp(): Express {
  const app = express();

  app.get('/events', (req, res) => {
    const cursor = typeof req.query.cursor === 'string' ? Number(req.query.cursor) : 0;
    const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 50;
    const start = Number.isNaN(cursor) ? 0 : cursor;
    const page = ALL_EVENTS.slice(start, start + limit);
    const nextCursor = String(start + page.length);

    res.status(200).json({ events: page, nextCursor });
  });

  return app;
}

if (require.main === module) {
  const app = createMockPartnerApiApp();
  const port = Number(process.env.PORT ?? 4002);
  app.listen(port, () => {
    console.log(`mock-partner-api listening on port ${port}`);
  });
}
```

- [ ] **Step 2: Write the failing test for `HttpPartnerEventsApiClient`**

`tests/unit/adapters/HttpPartnerEventsApiClient.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, Server } from 'node:http';
import { createMockPartnerApiApp } from '../../../mock-partner-api/server';
import { HttpPartnerEventsApiClient } from '../../../src/adapters/outbound/partner-api/HttpPartnerEventsApiClient';

let server: Server;
let baseUrl: string;
let client: HttpPartnerEventsApiClient;

beforeAll(async () => {
  server = createServer(createMockPartnerApiApp());
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('failed to determine test server address');
  }
  baseUrl = `http://127.0.0.1:${address.port}`;
  client = new HttpPartnerEventsApiClient(baseUrl);
});

afterAll(() => {
  server.close();
});

describe('HttpPartnerEventsApiClient', () => {
  it('fetches the first page when no cursor is given', async () => {
    const page = await client.listEvents(null, 10);

    expect(page.events).toHaveLength(10);
    expect(page.events[0]).toEqual({
      eventId: 'partner_evt_0',
      paymentId: 'partner_pay_0',
      type: 'payment.failed',
    });
    expect(page.nextCursor).toBe('10');
  });

  it('fetches a subsequent page using the given cursor', async () => {
    const page = await client.listEvents('10', 10);

    expect(page.events[0].eventId).toBe('partner_evt_10');
    expect(page.events[0].paymentId).toBe('partner_pay_10');
    expect(page.nextCursor).toBe('20');
  });

  it('returns a partial page near the end of the list', async () => {
    const page = await client.listEvents('115', 10);

    expect(page.events).toHaveLength(5);
    expect(page.nextCursor).toBe('120');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npx vitest run tests/unit/adapters/HttpPartnerEventsApiClient.test.ts`
Expected: FAIL — cannot find module `../../../src/adapters/outbound/partner-api/HttpPartnerEventsApiClient`.

- [ ] **Step 4: Implement `HttpPartnerEventsApiClient`**

`src/adapters/outbound/partner-api/HttpPartnerEventsApiClient.ts`:

```ts
import { z } from 'zod';
import { PartnerEventsApiPort, PartnerEventsPage } from '../../../application/ports/PartnerEventsApiPort';

const partnerEventSchema = z.object({
  eventId: z.string().min(1),
  paymentId: z.string().min(1),
  type: z.enum(['payment.succeeded', 'payment.failed']),
});

const partnerEventsPageSchema = z.object({
  events: z.array(partnerEventSchema),
  nextCursor: z.string().nullable(),
});

export class HttpPartnerEventsApiClient implements PartnerEventsApiPort {
  constructor(private readonly baseUrl: string) {}

  async listEvents(cursor: string | null, limit: number): Promise<PartnerEventsPage> {
    const url = new URL('/events', this.baseUrl);
    if (cursor !== null) {
      url.searchParams.set('cursor', cursor);
    }
    url.searchParams.set('limit', String(limit));

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`partner events API responded with status ${response.status}`);
    }

    return partnerEventsPageSchema.parse(await response.json());
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npx vitest run tests/unit/adapters/HttpPartnerEventsApiClient.test.ts`
Expected: PASS (3 tests).

- [ ] **Step 6: Commit**

```bash
git add mock-partner-api/ src/adapters/outbound/partner-api/ tests/unit/adapters/HttpPartnerEventsApiClient.test.ts
git commit -m "feat: add mock partner API and HttpPartnerEventsApiClient adapter"
```

---

### Task 15: Inbound HTTP adapters (TDD)

**Files:**
- Create: `src/adapters/inbound/http/webhookRouter.ts`
- Create: `src/adapters/inbound/http/dlqRouter.ts`
- Create: `src/adapters/inbound/http/reconciliationRouter.ts`
- Create: `src/adapters/inbound/http/app.ts`
- Test: `tests/unit/adapters/app.test.ts`

**Interfaces:**
- Consumes: `IngestWebhookEvent` (Task 6); `InvalidPaymentEventError` (Task 3); `DlqRepositoryPort` (Task 4); `ReprocessDlqEvent`, `DlqEntryNotFoundError` (Task 8); `ReconcilePayments` (Task 9); fakes (Task 5)
- Produces:
  - `createWebhookRouter(ingestWebhookEvent: IngestWebhookEvent): Router`
  - `createDlqRouter(dlqRepository: DlqRepositoryPort, reprocessDlqEvent: ReprocessDlqEvent): Router`
  - `createReconciliationRouter(reconcilePayments: ReconcilePayments): Router`
  - `AppDependencies { ingestWebhookEvent: IngestWebhookEvent; dlqRepository: DlqRepositoryPort; reprocessDlqEvent: ReprocessDlqEvent; reconcilePayments: ReconcilePayments }`
  - `createApp(deps: AppDependencies): Express`

The 4-argument Express error-handling middleware (JSON errors, malformed-JSON handling) is built into `app.ts` here, from the start.

- [ ] **Step 1: Write the failing tests**

`tests/unit/adapters/app.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import request from 'supertest';
import { createApp } from '../../../src/adapters/inbound/http/app';
import { IngestWebhookEvent } from '../../../src/application/use-cases/IngestWebhookEvent';
import { ReprocessDlqEvent } from '../../../src/application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from '../../../src/application/use-cases/ReconcilePayments';
import { FakeEventPublisherPort } from '../../fakes/FakeEventPublisherPort';
import { FakeDlqRepositoryPort } from '../../fakes/FakeDlqRepositoryPort';
import { FakePartnerEventsApiPort } from '../../fakes/FakePartnerEventsApiPort';
import { FakeReconciliationCursorPort } from '../../fakes/FakeReconciliationCursorPort';

function buildApp() {
  const eventPublisher = new FakeEventPublisherPort();
  const dlqRepository = new FakeDlqRepositoryPort();
  const ingestWebhookEvent = new IngestWebhookEvent(eventPublisher);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, eventPublisher);
  const partnerEventsApi = new FakePartnerEventsApiPort({ start: { events: [], nextCursor: null } });
  const cursorRepository = new FakeReconciliationCursorPort();
  const reconcilePayments = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository);

  const app = createApp({ ingestWebhookEvent, dlqRepository, reprocessDlqEvent, reconcilePayments });
  return { app, eventPublisher, dlqRepository };
}

describe('POST /webhooks/payments', () => {
  it('returns 202 and publishes a valid webhook payload', async () => {
    const { app, eventPublisher } = buildApp();

    const response = await request(app)
      .post('/webhooks/payments')
      .send({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(response.status).toBe(202);
    expect(eventPublisher.published).toEqual([{ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' }]);
  });

  it('returns 400 for an invalid webhook payload', async () => {
    const { app } = buildApp();

    const response = await request(app).post('/webhooks/payments').send({ paymentId: 'pay_1' });

    expect(response.status).toBe(400);
    expect(response.body.error).toBe('invalid webhook payload');
  });

  it('returns 400 with a JSON error when the request body is malformed JSON', async () => {
    const { app } = buildApp();

    const response = await request(app)
      .post('/webhooks/payments')
      .set('Content-Type', 'application/json')
      .send('{not valid json');

    expect(response.status).toBe(400);
    expect(response.body.error).toBe('invalid JSON body');
  });
});

describe('GET /dlq and POST /dlq/:id/reprocess', () => {
  it('lists unresolved DLQ entries', async () => {
    const { app, dlqRepository } = buildApp();
    await dlqRepository.add({
      eventKey: 'evt_1',
      eventType: 'payment.succeeded',
      payload: { eventId: 'evt_1' },
      failureReason: 'boom',
      attempts: 3,
    });

    const response = await request(app).get('/dlq');

    expect(response.status).toBe(200);
    expect(response.body.entries).toHaveLength(1);
    expect(response.body.entries[0].failureReason).toBe('boom');
  });

  it('reprocesses a DLQ entry by id', async () => {
    const { app, dlqRepository, eventPublisher } = buildApp();
    const id = await dlqRepository.add({
      eventKey: 'evt_1',
      eventType: 'payment.succeeded',
      payload: { eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' },
      failureReason: 'boom',
      attempts: 3,
    });

    const response = await request(app).post(`/dlq/${id}/reprocess`);

    expect(response.status).toBe(200);
    expect(eventPublisher.published).toEqual([{ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' }]);
  });

  it('returns 404 when reprocessing an unknown DLQ id', async () => {
    const { app } = buildApp();

    const response = await request(app).post('/dlq/missing-id/reprocess');

    expect(response.status).toBe(404);
  });
});

describe('POST /reconciliation/run', () => {
  it('returns 200 when reconciliation completes', async () => {
    const { app } = buildApp();

    const response = await request(app).post('/reconciliation/run');

    expect(response.status).toBe(200);
    expect(response.body.status).toBe('completed');
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npx vitest run tests/unit/adapters/app.test.ts`
Expected: FAIL — cannot find module `../../../src/adapters/inbound/http/app`.

- [ ] **Step 3: Implement `webhookRouter`**

`src/adapters/inbound/http/webhookRouter.ts`:

```ts
import { Router, Request, Response, NextFunction } from 'express';
import { IngestWebhookEvent } from '../../../application/use-cases/IngestWebhookEvent';
import { InvalidPaymentEventError } from '../../../domain/services/PaymentEventValidator';

export function createWebhookRouter(ingestWebhookEvent: IngestWebhookEvent): Router {
  const router = Router();

  router.post('/webhooks/payments', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await ingestWebhookEvent.execute(req.body);
      res.status(202).json({ status: 'accepted' });
    } catch (error) {
      if (error instanceof InvalidPaymentEventError) {
        res.status(400).json({ error: 'invalid webhook payload', details: error.message });
        return;
      }
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 4: Implement `dlqRouter`**

`src/adapters/inbound/http/dlqRouter.ts`:

```ts
import { Router, Request, Response, NextFunction } from 'express';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent, DlqEntryNotFoundError } from '../../../application/use-cases/ReprocessDlqEvent';

export function createDlqRouter(dlqRepository: DlqRepositoryPort, reprocessDlqEvent: ReprocessDlqEvent): Router {
  const router = Router();

  router.get('/dlq', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      const entries = await dlqRepository.list({ limit, offset });
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  router.post('/dlq/:id/reprocess', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await reprocessDlqEvent.execute(req.params.id);
      res.status(200).json({ status: 'reprocessed' });
    } catch (error) {
      if (error instanceof DlqEntryNotFoundError) {
        res.status(404).json({ error: 'DLQ entry not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 5: Implement `reconciliationRouter`**

`src/adapters/inbound/http/reconciliationRouter.ts`:

```ts
import { Router, Request, Response, NextFunction } from 'express';
import { ReconcilePayments } from '../../../application/use-cases/ReconcilePayments';

export function createReconciliationRouter(reconcilePayments: ReconcilePayments): Router {
  const router = Router();

  router.post('/reconciliation/run', async (_req: Request, res: Response, next: NextFunction) => {
    try {
      await reconcilePayments.execute();
      res.status(200).json({ status: 'completed' });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
```

- [ ] **Step 6: Implement `createApp`**

`src/adapters/inbound/http/app.ts`:

```ts
import express, { Express, Request, Response, NextFunction } from 'express';
import { IngestWebhookEvent } from '../../../application/use-cases/IngestWebhookEvent';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from '../../../application/use-cases/ReconcilePayments';
import { createWebhookRouter } from './webhookRouter';
import { createDlqRouter } from './dlqRouter';
import { createReconciliationRouter } from './reconciliationRouter';

export interface AppDependencies {
  ingestWebhookEvent: IngestWebhookEvent;
  dlqRepository: DlqRepositoryPort;
  reprocessDlqEvent: ReprocessDlqEvent;
  reconcilePayments: ReconcilePayments;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());
  app.use(createWebhookRouter(deps.ingestWebhookEvent));
  app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
  app.use(createReconciliationRouter(deps.reconcilePayments));

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `npx vitest run tests/unit/adapters/app.test.ts`
Expected: PASS (7 tests).

- [ ] **Step 8: Commit**

```bash
git add src/adapters/inbound/http/ tests/unit/adapters/app.test.ts
git commit -m "feat: add inbound HTTP adapters with JSON error handling"
```

---

### Task 16: Inbound Kafka consumer wiring

**Files:**
- Create: `src/adapters/inbound/kafka/paymentEventConsumer.ts`

**Interfaces:**
- Consumes: `ProcessPaymentEvent` (Task 7); `PAYMENT_EVENTS_TOPIC` (Task 13); the `kafkajs` package's `Consumer`
- Produces: `startPaymentEventConsumer(consumer: Consumer, processPaymentEvent: ProcessPaymentEvent): Promise<void>`

This is thin wiring with no branching logic of its own — it parses the raw Kafka message and delegates entirely to `ProcessPaymentEvent`, which already has full test coverage (Task 7). It is verified by the integration tests in Task 18, not an isolated unit test here.

- [ ] **Step 1: Implement `paymentEventConsumer`**

`src/adapters/inbound/kafka/paymentEventConsumer.ts`:

```ts
import { Consumer } from 'kafkajs';
import { ProcessPaymentEvent } from '../../../application/use-cases/ProcessPaymentEvent';
import { PAYMENT_EVENTS_TOPIC } from '../../outbound/kafka/KafkaEventPublisher';

export async function startPaymentEventConsumer(
  consumer: Consumer,
  processPaymentEvent: ProcessPaymentEvent,
): Promise<void> {
  await consumer.subscribe({ topic: PAYMENT_EVENTS_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await processPaymentEvent.execute(rawPayload);
    },
  });
}
```

- [ ] **Step 2: Verify tooling**

Run: `npm run typecheck && npm run lint`
Expected: both exit 0.

- [ ] **Step 3: Commit**

```bash
git add src/adapters/inbound/kafka/
git commit -m "feat: add Kafka consumer wiring for payment events"
```

---

### Task 17: Composition root (`main.ts`)

**Files:**
- Create: `src/main.ts`

**Interfaces:**
- Consumes: `loadEnv` (Task 10); `createApp` (Task 15); `createKafka`, `KafkaEventPublisher` (Task 13); `startPaymentEventConsumer` (Task 16); `PostgresPaymentRepository`, `PostgresDlqRepository`, `PostgresReconciliationCursorRepository` (Task 12); `RedisDedupeStore` (Task 11); `HttpPartnerEventsApiClient` (Task 14); `IngestWebhookEvent`, `ProcessPaymentEvent`, `ReprocessDlqEvent`, `ReconcilePayments` (Tasks 6-9)
- Produces: a running HTTP server, a running Kafka consumer, and a periodic reconciliation interval. No exported symbols consumed by later tasks (Task 18 builds its own composition for the tests, the same pattern as `kyc-risk-decision-engine`).

This task wires already-tested units together and starts a real process — verified by running it against the live infrastructure and issuing real requests, not by a unit test.

- [ ] **Step 1: Implement the composition root**

`src/main.ts`:

```ts
import { Pool } from 'pg';
import { createClient } from 'redis';
import { loadEnv } from './config/env';
import { createApp } from './adapters/inbound/http/app';
import { createKafka, KafkaEventPublisher } from './adapters/outbound/kafka/KafkaEventPublisher';
import { startPaymentEventConsumer } from './adapters/inbound/kafka/paymentEventConsumer';
import { PostgresPaymentRepository } from './adapters/outbound/postgres/PostgresPaymentRepository';
import { PostgresDlqRepository } from './adapters/outbound/postgres/PostgresDlqRepository';
import { PostgresReconciliationCursorRepository } from './adapters/outbound/postgres/PostgresReconciliationCursorRepository';
import { RedisDedupeStore } from './adapters/outbound/redis/RedisDedupeStore';
import { HttpPartnerEventsApiClient } from './adapters/outbound/partner-api/HttpPartnerEventsApiClient';
import { IngestWebhookEvent } from './application/use-cases/IngestWebhookEvent';
import { ProcessPaymentEvent } from './application/use-cases/ProcessPaymentEvent';
import { ReprocessDlqEvent } from './application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from './application/use-cases/ReconcilePayments';

async function main(): Promise<void> {
  const env = loadEnv();

  const pool = new Pool({ connectionString: env.databaseUrl });
  const redisClient = createClient({ url: env.redisUrl });
  await redisClient.connect();

  const kafka = createKafka(env.kafkaBrokers);
  const producer = kafka.producer();
  await producer.connect();
  const consumer = kafka.consumer({ groupId: 'webhook-ingestion-replay-dlq' });
  await consumer.connect();

  const eventPublisher = new KafkaEventPublisher(producer);
  const paymentRepository = new PostgresPaymentRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const cursorRepository = new PostgresReconciliationCursorRepository(pool);
  const dedupeStore = new RedisDedupeStore(redisClient);
  const partnerEventsApi = new HttpPartnerEventsApiClient(env.partnerApiBaseUrl);

  const ingestWebhookEvent = new IngestWebhookEvent(eventPublisher);
  const processPaymentEvent = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, eventPublisher);
  const reconcilePayments = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository);

  await startPaymentEventConsumer(consumer, processPaymentEvent);

  const app = createApp({ ingestWebhookEvent, dlqRepository, reprocessDlqEvent, reconcilePayments });
  app.listen(env.port, () => {
    console.log(`webhook-ingestion-replay-dlq listening on port ${env.port}`);
  });

  setInterval(() => {
    reconcilePayments.execute().catch((error) => {
      console.error('reconciliation run failed', error);
    });
  }, env.reconciliationIntervalMs);
}

main().catch((error) => {
  console.error('fatal startup error', error);
  process.exit(1);
});
```

- [ ] **Step 2: Manually verify the wiring**

Confirm infrastructure is up and migrated: `docker compose ps` shows `postgres`, `redis`, `kafka` running; `npm run migrate` has been run.

In one terminal:

Run: `PORT=4002 npm run mock-partner-api`
Expected: logs `mock-partner-api listening on port 4002`.

In a second terminal:

Run: `RECONCILIATION_INTERVAL_MS=3600000 npm run dev`
Expected: logs `webhook-ingestion-replay-dlq listening on port 3000`.

In a third terminal:

Run: `curl -s -X POST http://localhost:3000/webhooks/payments -H 'Content-Type: application/json' -d '{"eventId":"evt_demo_1","paymentId":"pay_demo_1","type":"payment.succeeded"}'`
Expected: `{"status":"accepted"}`, HTTP 202.

Run (after a second or two, to let the consumer process it): `docker compose exec -T postgres psql -U postgres -d webhook_ingestion -c "SELECT * FROM payments WHERE payment_id = 'pay_demo_1'"`
Expected: one row with `status = payment.succeeded`.

Run: `curl -s -X POST http://localhost:3000/webhooks/payments -H 'Content-Type: application/json' -d '{"eventId":"evt_demo_2","paymentId":"pay_fail_demo","type":"payment.succeeded"}'`
Expected: `202` (accepted at ingestion; the failure happens downstream in the consumer).

Run (after the retries complete, a few hundred ms): `curl -s http://localhost:3000/dlq`
Expected: JSON body with one `entries` item whose `failureReason` mentions "simulated processing failure".

Stop both servers (Ctrl+C in each terminal) once verified.

- [ ] **Step 3: Commit**

```bash
git add src/main.ts
git commit -m "feat: add composition root"
```

---

### Task 18: End-to-end integration tests (real Kafka, Postgres, Redis)

**Files:**
- Test: `tests/integration/webhookFlow.test.ts`
- Test: `tests/integration/dlqReplayFlow.test.ts`

**Interfaces:**
- Consumes: everything from Tasks 4-16 (`createApp`, all use cases, all real adapters, `startPaymentEventConsumer`, `FakePartnerEventsApiPort` for the reconciliation dependency these tests don't otherwise exercise)

These tests wire real Postgres, Redis, and Kafka into the real HTTP app and a real running consumer — proving the whole stack works end to end, the same way `kyc-risk-decision-engine`'s integration test proved its stack, but here the infrastructure is real rather than fakeable with a local HTTP server. Neither test imports `src/main.ts` (which calls `app.listen`/`process.exit` on import) — both rebuild equivalent composition inline.

- [ ] **Step 1: Write the happy-path integration test**

`tests/integration/webhookFlow.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import { Pool } from 'pg';
import { createClient } from 'redis';
import { Express } from 'express';
import { createApp } from '../../src/adapters/inbound/http/app';
import { IngestWebhookEvent } from '../../src/application/use-cases/IngestWebhookEvent';
import { ProcessPaymentEvent } from '../../src/application/use-cases/ProcessPaymentEvent';
import { ReprocessDlqEvent } from '../../src/application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from '../../src/application/use-cases/ReconcilePayments';
import {
  KafkaEventPublisher,
  createKafka,
  PAYMENT_EVENTS_TOPIC,
} from '../../src/adapters/outbound/kafka/KafkaEventPublisher';
import { PostgresPaymentRepository } from '../../src/adapters/outbound/postgres/PostgresPaymentRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { PostgresReconciliationCursorRepository } from '../../src/adapters/outbound/postgres/PostgresReconciliationCursorRepository';
import { RedisDedupeStore } from '../../src/adapters/outbound/redis/RedisDedupeStore';
import { startPaymentEventConsumer } from '../../src/adapters/inbound/kafka/paymentEventConsumer';
import { FakePartnerEventsApiPort } from '../fakes/FakePartnerEventsApiPort';

const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion';
const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;
let redisClient: ReturnType<typeof createClient>;
let app: Express;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL });
  await pool.query('TRUNCATE payments, dlq_events, reconciliation_cursor');

  redisClient = createClient({ url: REDIS_URL });
  await redisClient.connect();
  await redisClient.flushDb();

  const kafka = createKafka(KAFKA_BROKERS);
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: PAYMENT_EVENTS_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  const eventPublisher = new KafkaEventPublisher(producer);

  const paymentRepository = new PostgresPaymentRepository(pool);
  const dlqRepository = new PostgresDlqRepository(pool);
  const cursorRepository = new PostgresReconciliationCursorRepository(pool);
  const dedupeStore = new RedisDedupeStore(redisClient);

  const processPaymentEvent = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore);

  const consumer = kafka.consumer({ groupId: `webhook-flow-test-${Date.now()}` });
  await consumer.connect();
  await startPaymentEventConsumer(consumer, processPaymentEvent);

  const ingestWebhookEvent = new IngestWebhookEvent(eventPublisher);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, eventPublisher);
  const reconcilePayments = new ReconcilePayments(
    new FakePartnerEventsApiPort({ start: { events: [], nextCursor: null } }),
    eventPublisher,
    cursorRepository,
  );

  app = createApp({ ingestWebhookEvent, dlqRepository, reprocessDlqEvent, reconcilePayments });
}, 30000);

afterAll(async () => {
  await redisClient.quit();
  await pool.end();
});

describe('Webhook ingestion flow (integration)', () => {
  it('ingests a webhook and the payment lands in Postgres via the real Kafka consumer', async () => {
    const response = await request(app)
      .post('/webhooks/payments')
      .send({ eventId: 'evt_flow_1', paymentId: 'pay_flow_1', type: 'payment.succeeded' });

    expect(response.status).toBe(202);

    const row = await waitFor(async () => {
      const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_flow_1']);
      return result.rows[0] ?? null;
    });

    expect(row.status).toBe('payment.succeeded');
  }, 30000);

  it('does not duplicate the effect when the same event is published twice', async () => {
    const payload = { eventId: 'evt_flow_2', paymentId: 'pay_flow_2', type: 'payment.succeeded' };

    await request(app).post('/webhooks/payments').send(payload);
    await waitFor(async () => {
      const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_flow_2']);
      return result.rows[0] ?? null;
    });

    await request(app).post('/webhooks/payments').send(payload);
    // give the (idempotent) second delivery a moment to be processed/skipped
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_flow_2']);
    expect(result.rows).toHaveLength(1);
  }, 30000);
});
```

- [ ] **Step 2: Run the happy-path test**

Run: `npx vitest run tests/integration/webhookFlow.test.ts`
Expected: PASS (2 tests), against real Kafka, Redis, and Postgres.

- [ ] **Step 3: Write the DLQ replay integration test**

`tests/integration/dlqReplayFlow.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import request from 'supertest';
import { Pool } from 'pg';
import { createClient } from 'redis';
import { Express } from 'express';
import { createApp } from '../../src/adapters/inbound/http/app';
import { IngestWebhookEvent } from '../../src/application/use-cases/IngestWebhookEvent';
import { ProcessPaymentEvent } from '../../src/application/use-cases/ProcessPaymentEvent';
import { ReprocessDlqEvent } from '../../src/application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from '../../src/application/use-cases/ReconcilePayments';
import {
  KafkaEventPublisher,
  createKafka,
  PAYMENT_EVENTS_TOPIC,
} from '../../src/adapters/outbound/kafka/KafkaEventPublisher';
import { PostgresPaymentRepository } from '../../src/adapters/outbound/postgres/PostgresPaymentRepository';
import { PostgresDlqRepository } from '../../src/adapters/outbound/postgres/PostgresDlqRepository';
import { PostgresReconciliationCursorRepository } from '../../src/adapters/outbound/postgres/PostgresReconciliationCursorRepository';
import { RedisDedupeStore } from '../../src/adapters/outbound/redis/RedisDedupeStore';
import { startPaymentEventConsumer } from '../../src/adapters/inbound/kafka/paymentEventConsumer';
import { FakePartnerEventsApiPort } from '../fakes/FakePartnerEventsApiPort';

const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/webhook_ingestion';
const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379';
const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let pool: Pool;
let redisClient: ReturnType<typeof createClient>;
let app: Express;
let dlqRepository: PostgresDlqRepository;

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 10000): Promise<T> {
  const start = Date.now();
  for (;;) {
    const result = await check();
    if (result !== null) {
      return result;
    }
    if (Date.now() - start > timeoutMs) {
      throw new Error('waitFor timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
}

beforeAll(async () => {
  pool = new Pool({ connectionString: DATABASE_URL });
  await pool.query('TRUNCATE payments, dlq_events, reconciliation_cursor');

  redisClient = createClient({ url: REDIS_URL });
  await redisClient.connect();
  await redisClient.flushDb();

  const kafka = createKafka(KAFKA_BROKERS);
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: PAYMENT_EVENTS_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  const producer = kafka.producer();
  await producer.connect();
  const eventPublisher = new KafkaEventPublisher(producer);

  const paymentRepository = new PostgresPaymentRepository(pool);
  dlqRepository = new PostgresDlqRepository(pool);
  const cursorRepository = new PostgresReconciliationCursorRepository(pool);
  const dedupeStore = new RedisDedupeStore(redisClient);

  const processPaymentEvent = new ProcessPaymentEvent(paymentRepository, dlqRepository, dedupeStore, {
    maxAttempts: 1,
    backoffMs: () => 0,
  });

  const consumer = kafka.consumer({ groupId: `dlq-replay-flow-test-${Date.now()}` });
  await consumer.connect();
  await startPaymentEventConsumer(consumer, processPaymentEvent);

  const ingestWebhookEvent = new IngestWebhookEvent(eventPublisher);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, eventPublisher);
  const reconcilePayments = new ReconcilePayments(
    new FakePartnerEventsApiPort({ start: { events: [], nextCursor: null } }),
    eventPublisher,
    cursorRepository,
  );

  app = createApp({ ingestWebhookEvent, dlqRepository, reprocessDlqEvent, reconcilePayments });
}, 30000);

afterAll(async () => {
  await redisClient.quit();
  await pool.end();
});

describe('DLQ replay flow (integration)', () => {
  it('sends a failing payment to the DLQ, then successfully replays it after the payload is fixed', async () => {
    const response = await request(app)
      .post('/webhooks/payments')
      .send({ eventId: 'evt_dlq_1', paymentId: 'pay_fail_demo', type: 'payment.succeeded' });
    expect(response.status).toBe(202);

    const dlqEntry = await waitFor(async () => {
      const entries = await dlqRepository.list({ limit: 10, offset: 0 });
      return entries.find((entry) => entry.eventKey === 'evt_dlq_1') ?? null;
    });
    expect(dlqEntry.failureReason).toContain('simulated processing failure');

    // Simulate an operator fixing the bad paymentId before reprocessing.
    await pool.query(`UPDATE dlq_events SET payload = $1 WHERE id = $2`, [
      JSON.stringify({ eventId: 'evt_dlq_1', paymentId: 'pay_1', type: 'payment.succeeded' }),
      dlqEntry.id,
    ]);

    const reprocessResponse = await request(app).post(`/dlq/${dlqEntry.id}/reprocess`);
    expect(reprocessResponse.status).toBe(200);

    await waitFor(async () => {
      const result = await pool.query('SELECT status FROM payments WHERE payment_id = $1', ['pay_1']);
      return result.rows[0] ?? null;
    });

    const finalDlqEntry = await dlqRepository.get(dlqEntry.id);
    expect(finalDlqEntry?.reprocessedAt).not.toBeNull();
  }, 30000);
});
```

- [ ] **Step 4: Run the full test suite**

Run: `npm test`
Expected: all suites pass, including both new integration tests. Total test count across the whole suite: 4 (PaymentEventValidator) + 2 (IngestWebhookEvent) + 5 (ProcessPaymentEvent) + 2 (ReprocessDlqEvent) + 4 (ReconcilePayments) + 4 (env) + 3 (RedisDedupeStore) + 8 (PostgresRepositories) + 1 (KafkaEventPublisher) + 3 (HttpPartnerEventsApiClient) + 7 (app) + 2 (webhookFlow) + 1 (dlqReplayFlow) = 46 tests.

- [ ] **Step 5: Commit**

```bash
git add tests/integration/
git commit -m "test: add end-to-end integration tests for webhook flow and DLQ replay"
```

---

### Task 19: Docker, Docker Compose finalization, and README usage instructions

**Files:**
- Create: `.dockerignore`
- Create: `Dockerfile`
- Modify: `docker-compose.yml`
- Modify: `README.md`

**Interfaces:**
- None (deployment/documentation only)

- [ ] **Step 1: Create `.dockerignore`**

```
node_modules
.git
.worktrees
.superpowers
coverage
```

- [ ] **Step 2: Create the `Dockerfile`**

```dockerfile
FROM node:22-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

EXPOSE 3000
EXPOSE 4002

CMD ["npx", "tsx", "src/main.ts"]
```

- [ ] **Step 3: Add the `app`, `mock-partner-api`, and `migrate` services to `docker-compose.yml`**

Read the current `docker-compose.yml` first (from Task 2), then append these three services (keep `postgres`, `redis`, and `kafka` exactly as they are):

```yaml
  mock-partner-api:
    build: .
    command: ["npx", "tsx", "mock-partner-api/server.ts"]
    environment:
      PORT: "4002"
    ports:
      - "4002:4002"

  migrate:
    build: .
    command: ["npx", "tsx", "db/migrate.ts"]
    environment:
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/webhook_ingestion"
    depends_on:
      postgres:
        condition: service_healthy

  app:
    build: .
    command: ["npx", "tsx", "src/main.ts"]
    environment:
      PORT: "3000"
      DATABASE_URL: "postgres://postgres:postgres@postgres:5432/webhook_ingestion"
      REDIS_URL: "redis://redis:6379"
      KAFKA_BROKERS: "kafka:29092"
      PARTNER_API_BASE_URL: "http://mock-partner-api:4002"
      RECONCILIATION_INTERVAL_MS: "3600000"
    ports:
      - "3000:3000"
    depends_on:
      postgres:
        condition: service_healthy
      migrate:
        condition: service_completed_successfully
```

- [ ] **Step 4: Verify with Docker Compose**

Run: `docker compose down -v` (clean slate, including the volumes from earlier manual testing)
Run: `docker compose up --build -d`
Expected: `postgres`, `redis`, `kafka` start; `migrate` runs to completion and exits 0; `app` and `mock-partner-api` start and stay running. Confirm with `docker compose ps`.

Run: `curl -s -X POST http://localhost:3000/webhooks/payments -H 'Content-Type: application/json' -d '{"eventId":"evt_docker_1","paymentId":"pay_docker_1","type":"payment.succeeded"}'`
Expected: `202` with `{"status":"accepted"}`.

Run (after a couple of seconds): `curl -s http://localhost:3000/dlq`
Expected: `200` with `{"entries":[]}` (the demo payload wasn't the magic failure trigger, so nothing landed in the DLQ).

Run: `docker compose down`
Expected: containers stop and are removed.

- [ ] **Step 5: Add usage instructions to `README.md`**

Read the current `README.md` first, then add this section right after the existing "## Kubernetes" section (keep all existing content):

```markdown
## How to run

Locally, without Docker:

\`\`\`bash
npm install
npm run infra:up      # starts Postgres, Redis, Kafka
npm run migrate       # applies the database schema
npm run mock-partner-api   # terminal 1 — synthetic partner events on port 4002
npm run dev                 # terminal 2 — main service on port 3000
\`\`\`

With Docker Compose (everything, including the app itself):

\`\`\`bash
docker compose up --build
\`\`\`

Testing:

\`\`\`bash
curl -X POST http://localhost:3000/webhooks/payments \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt_demo_1","paymentId":"pay_demo_1","type":"payment.succeeded"}'

curl -X POST http://localhost:3000/webhooks/payments \
  -H 'Content-Type: application/json' \
  -d '{"eventId":"evt_demo_2","paymentId":"pay_fail_demo","type":"payment.succeeded"}'

curl http://localhost:3000/dlq
\`\`\`

The second call uses the magic failure trigger (\`pay_fail_demo\`), which always fails processing and lands in the DLQ after 3 retries — use it to demonstrate the DLQ and reprocess flow on demand.

Reprocessing a DLQ entry:

\`\`\`bash
curl -X POST http://localhost:3000/dlq/<id>/reprocess
\`\`\`

Triggering reconciliation on demand:

\`\`\`bash
curl -X POST http://localhost:3000/reconciliation/run
\`\`\`

Running the tests (requires \`npm run infra:up\` and \`npm run migrate\` first):

\`\`\`bash
npm test
npm run typecheck
npm run lint
\`\`\`
```

- [ ] **Step 6: Final full verification**

Run: `npm run infra:up` (if not already running from Task 18), `npm run migrate`, then:

Run: `npm run typecheck && npm run lint && npm test`
Expected: all three exit 0.

- [ ] **Step 7: Commit**

```bash
git add .dockerignore Dockerfile docker-compose.yml README.md
git commit -m "chore: finalize Docker Compose deployment and usage instructions"
```
