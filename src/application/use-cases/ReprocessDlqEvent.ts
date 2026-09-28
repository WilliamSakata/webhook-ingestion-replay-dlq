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
