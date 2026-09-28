import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { Kafka, Consumer, Producer } from 'kafkajs';
import {
  KafkaEventPublisher,
  createKafka,
  PAYMENT_EVENTS_TOPIC,
} from '../../../src/adapters/outbound/kafka/KafkaEventPublisher';

const KAFKA_BROKERS = (process.env.TEST_KAFKA_BROKERS ?? 'localhost:9092').split(',');

let kafka: Kafka;
let publisher: KafkaEventPublisher;
let producer: Producer;
let consumer: Consumer;

beforeAll(async () => {
  kafka = createKafka(KAFKA_BROKERS);

  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({ topics: [{ topic: PAYMENT_EVENTS_TOPIC, numPartitions: 1 }] });
  await admin.disconnect();

  producer = kafka.producer();
  await producer.connect();
  publisher = new KafkaEventPublisher(producer);

  consumer = kafka.consumer({ groupId: `kafka-event-publisher-test-${Date.now()}` });
  await consumer.connect();
  await consumer.subscribe({ topic: PAYMENT_EVENTS_TOPIC, fromBeginning: false });
}, 30000);

afterAll(async () => {
  await consumer.disconnect();
  await producer.disconnect();
}, 30000);

describe('KafkaEventPublisher', () => {
  it('publishes an event that a consumer can read back', async () => {
    const received = new Promise<string>((resolve) => {
      consumer.run({
        eachMessage: async ({ message }) => {
          resolve(message.value?.toString() ?? '');
        },
      });
    });

    // give the consumer group a moment to finish joining before publishing
    await new Promise((resolve) => setTimeout(resolve, 1000));

    await publisher.publish({ eventId: 'evt_test_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    const value = await received;
    expect(JSON.parse(value)).toEqual({ eventId: 'evt_test_1', paymentId: 'pay_1', type: 'payment.succeeded' });
  }, 30000);
});
