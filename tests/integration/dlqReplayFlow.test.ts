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

  // give the consumer group time to finish joining before any test publishes
  // (Task 13 found ~3.4s join time on this machine; see that task's report)
  await new Promise((resolve) => setTimeout(resolve, 5000));

  const ingestWebhookEvent = new IngestWebhookEvent(eventPublisher);
  const reprocessDlqEvent = new ReprocessDlqEvent(dlqRepository, eventPublisher);
  const reconcilePayments = new ReconcilePayments(
    new FakePartnerEventsApiPort({ start: { events: [], nextCursor: null } }),
    eventPublisher,
    cursorRepository,
  );

  app = createApp({ ingestWebhookEvent, dlqRepository, reprocessDlqEvent, reconcilePayments });
}, 40000);

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
