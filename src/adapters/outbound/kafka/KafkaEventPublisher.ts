import { Kafka, Producer } from 'kafkajs';
import { EventPublisherPort } from '../../../application/ports/EventPublisherPort';
import { PaymentEvent } from '../../../domain/value-objects/PaymentEvent';

export const PAYMENT_EVENTS_TOPIC = 'payment-events';

export function createKafka(brokers: string[]): Kafka {
  return new Kafka({ clientId: 'webhook-ingestion-replay-dlq', brokers });
}

export class KafkaEventPublisher implements EventPublisherPort {
  constructor(private readonly producer: Producer) {}

  async publish(event: PaymentEvent): Promise<void> {
    await this.producer.send({
      topic: PAYMENT_EVENTS_TOPIC,
      messages: [{ key: event.eventId, value: JSON.stringify(event) }],
    });
  }
}
