import { validatePaymentEvent } from '../../domain/services/PaymentEventValidator';
import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';
import { PaymentRepositoryPort } from '../ports/PaymentRepositoryPort';
import { DlqRepositoryPort } from '../ports/DlqRepositoryPort';
import { DedupeStorePort } from '../ports/DedupeStorePort';

export interface ProcessPaymentEventOptions {
  maxAttempts?: number;
  backoffMs?: (attempt: number) => number;
  failOnPaymentId?: string;
}

const DEFAULT_BACKOFF_MS = (attempt: number): number => 100 * 2 ** (attempt - 1);

export class ProcessPaymentEvent {
  private readonly maxAttempts: number;
  private readonly backoffMs: (attempt: number) => number;
  private readonly failOnPaymentId: string;

  constructor(
    private readonly paymentRepository: PaymentRepositoryPort,
    private readonly dlqRepository: DlqRepositoryPort,
    private readonly dedupeStore: DedupeStorePort,
    options: ProcessPaymentEventOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 3;
    this.backoffMs = options.backoffMs ?? DEFAULT_BACKOFF_MS;
    this.failOnPaymentId = options.failOnPaymentId ?? 'pay_fail_demo';
  }

  async execute(rawPayload: unknown): Promise<void> {
    const dedupeKey = this.extractEventId(rawPayload);

    if (dedupeKey !== null) {
      const alreadyProcessed = await this.dedupeStore.wasProcessed(dedupeKey);
      if (alreadyProcessed) {
        return;
      }
    }

    let lastError: Error = new Error('unknown processing error');

    for (let attempt = 1; attempt <= this.maxAttempts; attempt += 1) {
      try {
        const event = validatePaymentEvent(rawPayload);
        this.assertShouldSucceed(event);
        await this.paymentRepository.upsertPaymentStatus(event.paymentId, event.type);
        await this.dedupeStore.markProcessed(event.eventId);
        return;
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        if (attempt < this.maxAttempts) {
          await this.delay(this.backoffMs(attempt));
        }
      }
    }

    await this.dlqRepository.add({
      eventKey: dedupeKey ?? 'unknown',
      eventType: this.extractEventType(rawPayload),
      payload: rawPayload,
      failureReason: lastError.message,
      attempts: this.maxAttempts,
    });
  }

  private assertShouldSucceed(event: PaymentEvent): void {
    if (event.paymentId === this.failOnPaymentId) {
      throw new Error(`simulated processing failure for paymentId "${event.paymentId}"`);
    }
  }

  private extractEventId(rawPayload: unknown): string | null {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'eventId' in rawPayload) {
      const value = (rawPayload as { eventId: unknown }).eventId;
      return typeof value === 'string' ? value : null;
    }
    return null;
  }

  private extractEventType(rawPayload: unknown): string {
    if (typeof rawPayload === 'object' && rawPayload !== null && 'type' in rawPayload) {
      const value = (rawPayload as { type: unknown }).type;
      return typeof value === 'string' ? value : 'unknown';
    }
    return 'unknown';
  }

  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
