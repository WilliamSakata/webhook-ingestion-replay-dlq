import { Pool } from 'pg';
import {
  ReconciliationCursorPort,
  ReconciliationCursorState,
} from '../../../application/ports/ReconciliationCursorPort';

const CURSOR_ID = 'partner_events';

export class PostgresReconciliationCursorRepository implements ReconciliationCursorPort {
  constructor(private readonly pool: Pool) {}

  async load(): Promise<ReconciliationCursorState | null> {
    const result = await this.pool.query<{ cursor: string | null; updated_at: Date }>(
      `SELECT cursor, updated_at FROM reconciliation_cursor WHERE id = $1`,
      [CURSOR_ID],
    );
    if (result.rows.length === 0) {
      return null;
    }
    return { cursor: result.rows[0].cursor, updatedAt: result.rows[0].updated_at };
  }

  async save(cursor: string | null): Promise<void> {
    await this.pool.query(
      `INSERT INTO reconciliation_cursor (id, cursor, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (id) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at`,
      [CURSOR_ID, cursor],
    );
  }
}
