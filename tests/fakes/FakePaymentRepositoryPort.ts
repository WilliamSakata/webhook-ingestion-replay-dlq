import { PaymentRepositoryPort } from '../../src/application/ports/PaymentRepositoryPort';
import { PaymentEventType } from '../../src/domain/value-objects/PaymentEvent';

export class FakePaymentRepositoryPort implements PaymentRepositoryPort {
  public upserts: { paymentId: string; status: PaymentEventType }[] = [];

  async upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void> {
    this.upserts.push({ paymentId, status });
  }
}
