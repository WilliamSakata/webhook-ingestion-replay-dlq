import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import { createClient } from 'redis';
import { RedisDedupeStore } from '../../../src/adapters/outbound/redis/RedisDedupeStore';

const REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379';

let client: ReturnType<typeof createClient>;
let store: RedisDedupeStore;

beforeAll(async () => {
  client = createClient({ url: REDIS_URL });
  await client.connect();
  store = new RedisDedupeStore(client);
});

afterAll(async () => {
  await client.quit();
});

beforeEach(async () => {
  await client.flushDb();
});

describe('RedisDedupeStore', () => {
  it('reports an eventId as not processed before it is marked', async () => {
    expect(await store.wasProcessed('evt_1')).toBe(false);
  });

  it('reports an eventId as processed after it is marked', async () => {
    await store.markProcessed('evt_1');

    expect(await store.wasProcessed('evt_1')).toBe(true);
  });

  it('keeps dedupe state independent per eventId', async () => {
    await store.markProcessed('evt_1');

    expect(await store.wasProcessed('evt_2')).toBe(false);
  });
});
