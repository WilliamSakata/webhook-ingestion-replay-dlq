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
