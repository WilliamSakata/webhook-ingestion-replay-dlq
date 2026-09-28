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
