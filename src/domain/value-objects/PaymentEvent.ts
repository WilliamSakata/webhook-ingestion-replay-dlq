export type PaymentEventType = 'payment.succeeded' | 'payment.failed';

export interface PaymentEvent {
  eventId: string;
  paymentId: string;
  type: PaymentEventType;
}
