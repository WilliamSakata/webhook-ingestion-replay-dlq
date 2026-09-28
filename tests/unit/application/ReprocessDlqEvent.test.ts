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
