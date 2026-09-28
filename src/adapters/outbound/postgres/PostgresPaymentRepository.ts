import { Pool } from 'pg';
import { PaymentRepositoryPort } from '../../../application/ports/PaymentRepositoryPort';
import { PaymentEventType } from '../../../domain/value-objects/PaymentEvent';

export class PostgresPaymentRepository implements PaymentRepositoryPort {
  constructor(private readonly pool: Pool) {}

  async upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void> {
    await this.pool.query(
      `INSERT INTO payments (payment_id, status, updated_at)
       VALUES ($1, $2, now())
       ON CONFLICT (payment_id) DO UPDATE SET status = excluded.status, updated_at = excluded.updated_at`,
      [paymentId, status],
    );
  }
}
