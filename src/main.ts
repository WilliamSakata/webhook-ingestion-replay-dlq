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
