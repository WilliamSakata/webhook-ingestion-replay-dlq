import { Pool } from 'pg';
import { DlqEntry, DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';

interface DlqRow {
  id: string;
  event_key: string;
  event_type: string;
  payload: unknown;
  failure_reason: string;
  attempts: number;
  created_at: Date;
  reprocessed_at: Date | null;
}

function toDlqEntry(row: DlqRow): DlqEntry {
  return {
    id: row.id,
    eventKey: row.event_key,
    eventType: row.event_type,
    payload: row.payload,
    failureReason: row.failure_reason,
    attempts: row.attempts,
    createdAt: row.created_at,
    reprocessedAt: row.reprocessed_at,
  };
}

export class PostgresDlqRepository implements DlqRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async add(entry: {
    eventKey: string;
    eventType: string;
    payload: unknown;
    failureReason: string;
    attempts: number;
  }): Promise<string> {
    const result = await this.pool.query<{ id: string }>(
      `INSERT INTO dlq_events (event_key, event_type, payload, failure_reason, attempts)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id`,
      [entry.eventKey, entry.eventType, JSON.stringify(entry.payload), entry.failureReason, entry.attempts],
    );
    return result.rows[0].id;
  }

  async list(options: { limit: number; offset: number }): Promise<DlqEntry[]> {
    const result = await this.pool.query<DlqRow>(
      `SELECT * FROM dlq_events WHERE reprocessed_at IS NULL ORDER BY created_at ASC LIMIT $1 OFFSET $2`,
      [options.limit, options.offset],
    );
    return result.rows.map(toDlqEntry);
  }

  async get(id: string): Promise<DlqEntry | null> {
    const result = await this.pool.query<DlqRow>(`SELECT * FROM dlq_events WHERE id = $1`, [id]);
    return result.rows[0] ? toDlqEntry(result.rows[0]) : null;
  }

  async markReprocessed(id: string): Promise<void> {
    await this.pool.query(`UPDATE dlq_events SET reprocessed_at = now() WHERE id = $1`, [id]);
  }
}
