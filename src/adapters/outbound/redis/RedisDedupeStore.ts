import { createClient } from 'redis';
import { DedupeStorePort } from '../../../application/ports/DedupeStorePort';

type RedisClient = ReturnType<typeof createClient>;

const DEDUPE_TTL_SECONDS = 24 * 60 * 60;

export class RedisDedupeStore implements DedupeStorePort {
  constructor(private readonly client: RedisClient) {}

  async wasProcessed(eventId: string): Promise<boolean> {
    const value = await this.client.get(this.key(eventId));
    return value !== null;
  }

  async markProcessed(eventId: string): Promise<void> {
    await this.client.set(this.key(eventId), '1', { EX: DEDUPE_TTL_SECONDS });
  }

  private key(eventId: string): string {
    return `dedupe:${eventId}`;
  }
}
