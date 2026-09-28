import {
  ReconciliationCursorPort,
  ReconciliationCursorState,
} from '../../src/application/ports/ReconciliationCursorPort';

export class FakeReconciliationCursorPort implements ReconciliationCursorPort {
  private state: ReconciliationCursorState | null;

  constructor(initial: ReconciliationCursorState | null = null) {
    this.state = initial;
  }

  async load(): Promise<ReconciliationCursorState | null> {
    return this.state;
  }

  async save(cursor: string | null): Promise<void> {
    this.state = { cursor, updatedAt: new Date() };
  }
}
