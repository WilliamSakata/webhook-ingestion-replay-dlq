import { PaymentEvent } from '../../domain/value-objects/PaymentEvent';

export interface PartnerEventsPage {
  events: PaymentEvent[];
  nextCursor: string | null;
}

export interface PartnerEventsApiPort {
  listEvents(cursor: string | null, limit: number): Promise<PartnerEventsPage>;
}
