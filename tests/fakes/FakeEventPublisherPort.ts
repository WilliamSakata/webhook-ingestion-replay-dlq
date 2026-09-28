import { EventPublisherPort } from '../../src/application/ports/EventPublisherPort';
import { PaymentEvent } from '../../src/domain/value-objects/PaymentEvent';

export class FakeEventPublisherPort implements EventPublisherPort {
  public published: PaymentEvent[] = [];

  async publish(event: PaymentEvent): Promise<void> {
    this.published.push(event);
  }
}
