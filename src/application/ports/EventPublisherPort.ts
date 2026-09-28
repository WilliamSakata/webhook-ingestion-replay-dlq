import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';

export interface EventPublisherPort {
  publish(event: PaymentEvent): Promise<void>;
}
