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

  it('returns 400 for an invalid limit query parameter', async () => {
    const { app } = buildApp();

    const response = await request(app).get('/dlq?limit=abc');

    expect(response.status).toBe(400);
    expect(response.body).toEqual({ error: 'invalid limit or offset' });
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
