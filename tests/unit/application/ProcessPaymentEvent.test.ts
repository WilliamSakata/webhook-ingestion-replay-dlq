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
