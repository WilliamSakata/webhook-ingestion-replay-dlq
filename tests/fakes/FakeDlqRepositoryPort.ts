import { randomUUID } from 'node:crypto';
import { DlqEntry, DlqRepositoryPort } from '../../src/application/ports/DlqRepositoryPort';

export class FakeDlqRepositoryPort implements DlqRepositoryPort {
  public entries: DlqEntry[] = [];

  async add(entry: {
    eventKey: string;
    eventType: string;
    payload: unknown;
    failureReason: string;
    attempts: number;
  }): Promise<string> {
    const id = randomUUID();
    this.entries.push({ id, ...entry, createdAt: new Date(), reprocessedAt: null });
    return id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    return this.entries
      .filter((entry) => entry.reprocessedAt === null)
      .slice(options.offset, options.offset + options.limit);
  }

  async get(id: string): Promise<DlqEntry | null> {
    return this.entries.find((entry) => entry.id === id) ?? null;
  }

  async markReprocessed(id: string): Promise<void> {
    const entry = this.entries.find((e) => e.id === id);
    if (entry) {
      entry.reprocessedAt = new Date();
    }
  }
}
