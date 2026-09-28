import { PaymentEventType } from '../../domain/value-objects/PaymentEvent';

export interface PaymentRepositoryPort {
  upsertPaymentStatus(paymentId: string, status: PaymentEventType): Promise<void>;
}
