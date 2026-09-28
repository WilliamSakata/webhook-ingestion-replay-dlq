import { DedupeStorePort } from '../../src/application/ports/DedupeStorePort';

export class FakeDedupeStorePort implements DedupeStorePort {
  private readonly processed = new Set<string>();

  async wasProcessed(eventId: string): Promise<boolean> {
    return this.processed.has(eventId);
  }

  async markProcessed(eventId: string): Promise<void> {
    this.processed.add(eventId);
  }
}
