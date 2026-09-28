import { validatePaymentEvent } from '../../domain/services/PaymentEventValidator';
import { EventPublisherPort } from '../ports/EventPublisherPort';

export class IngestWebhookEvent {
  constructor(private readonly eventPublisher: EventPublisherPort) {}

  async execute(rawPayload: unknown): Promise<void> {
    const event = validatePaymentEvent(rawPayload);
    await this.eventPublisher.publish(event);
  }
}
