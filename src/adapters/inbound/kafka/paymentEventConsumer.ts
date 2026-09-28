import { Consumer } from 'kafkajs';
import { ProcessPaymentEvent } from '../../../application/use-cases/ProcessPaymentEvent';
import { PAYMENT_EVENTS_TOPIC } from '../../outbound/kafka/KafkaEventPublisher';

export async function startPaymentEventConsumer(
  consumer: Consumer,
  processPaymentEvent: ProcessPaymentEvent,
): Promise<void> {
  await consumer.subscribe({ topic: PAYMENT_EVENTS_TOPIC, fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ message }) => {
      let rawPayload: unknown;
      try {
        rawPayload = JSON.parse(message.value?.toString() ?? '{}');
      } catch {
        rawPayload = {};
      }
      await processPaymentEvent.execute(rawPayload);
    },
  });
}
