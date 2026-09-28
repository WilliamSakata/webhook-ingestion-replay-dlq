import { z } from 'zod';
import { PartnerEventsApiPort, PartnerEventsPage } from '../../../application/ports/PartnerEventsApiPort';

const partnerEventSchema = z.object({
  eventId: z.string().min(1),
  paymentId: z.string().min(1),
  type: z.enum(['payment.succeeded', 'payment.failed']),
});

const partnerEventsPageSchema = z.object({
  events: z.array(partnerEventSchema),
  nextCursor: z.string().nullable(),
});

export class HttpPartnerEventsApiClient implements PartnerEventsApiPort {
  constructor(private readonly baseUrl: string) {}

  async listEvents(cursor: string | null, limit: number): Promise<PartnerEventsPage> {
    const url = new URL('/events', this.baseUrl);
    if (cursor !== null) {
      url.searchParams.set('cursor', cursor);
    }
    url.searchParams.set('limit', String(limit));

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`partner events API responded with status ${response.status}`);
    }

    return partnerEventsPageSchema.parse(await response.json());
  }
}
