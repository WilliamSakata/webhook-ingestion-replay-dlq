import { describe, it, expect } from 'vitest';
import { validatePaymentEvent, InvalidPaymentEventError } from '../../../src/domain/services/PaymentEventValidator';

describe('validatePaymentEvent', () => {
  it('parses a valid payment.succeeded event', () => {
    const event = validatePaymentEvent({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });

    expect(event).toEqual({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.succeeded' });
  });

  it('parses a valid payment.failed event', () => {
    const event = validatePaymentEvent({ eventId: 'evt_2', paymentId: 'pay_2', type: 'payment.failed' });

    expect(event).toEqual({ eventId: 'evt_2', paymentId: 'pay_2', type: 'payment.failed' });
  });

  it('throws InvalidPaymentEventError when paymentId is missing', () => {
    expect(() => validatePaymentEvent({ eventId: 'evt_1', type: 'payment.succeeded' })).toThrow(
      InvalidPaymentEventError,
    );
  });

  it('throws InvalidPaymentEventError when type is not a known value', () => {
    expect(() =>
      validatePaymentEvent({ eventId: 'evt_1', paymentId: 'pay_1', type: 'payment.pending' }),
    ).toThrow(InvalidPaymentEventError);
  });
});
