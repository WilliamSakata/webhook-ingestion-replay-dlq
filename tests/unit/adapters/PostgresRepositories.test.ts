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

  it('returns null from get for a malformed, non-UUID id instead of throwing', async () => {
    const repo = new PostgresDlqRepository(pool);

    expect(await repo.get('not-a-valid-uuid')).toBeNull();
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
