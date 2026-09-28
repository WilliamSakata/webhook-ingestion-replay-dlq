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

  it('stops after a full page whose nextCursor is null, instead of looping forever', async () => {
    const partnerEventsApi = new FakePartnerEventsApiPort({
      c1: { events: [event2], nextCursor: null },
    });
    const eventPublisher = new FakeEventPublisherPort();
    const cursorRepository = new FakeReconciliationCursorPort({ cursor: 'c1', updatedAt: new Date() });
    const useCase = new ReconcilePayments(partnerEventsApi, eventPublisher, cursorRepository, { pageSize: 1 });

    await useCase.execute();

    expect(eventPublisher.published).toEqual([event2]);
    expect((await cursorRepository.load())?.cursor).toBe('c1');
  });
});
