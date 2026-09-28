import { z } from 'zod';
import { PaymentEvent } from '../value-objects/PaymentEvent';

const paymentEventSchema = z.object({
  eventId: z.string().min(1),
  paymentId: z.string().min(1),
  type: z.enum(['payment.succeeded', 'payment.failed']),
});

export class InvalidPaymentEventError extends Error {}

export function validatePaymentEvent(raw: unknown): PaymentEvent {
  const result = paymentEventSchema.safeParse(raw);
  if (!result.success) {
    throw new InvalidPaymentEventError(result.error.message);
  }
  return result.data;
}
