import { PartnerEventsApiPort, PartnerEventsPage } from '../../src/application/ports/PartnerEventsApiPort';

export class FakePartnerEventsApiPort implements PartnerEventsApiPort {
  constructor(private readonly pages: Record<string, PartnerEventsPage>) {}

  async listEvents(cursor: string | null, _limit: number): Promise<PartnerEventsPage> {
    const key = cursor ?? 'start';
    const page = this.pages[key];
    if (!page) {
      throw new Error(`FakePartnerEventsApiPort: no page configured for cursor "${key}"`);
    }
    return page;
  }
}
