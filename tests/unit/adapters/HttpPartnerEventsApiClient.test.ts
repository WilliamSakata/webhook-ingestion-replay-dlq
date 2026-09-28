import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createServer, Server } from 'node:http';
import { createMockPartnerApiApp } from '../../../mock-partner-api/server';
import { HttpPartnerEventsApiClient } from '../../../src/adapters/outbound/partner-api/HttpPartnerEventsApiClient';

let server: Server;
let baseUrl: string;
let client: HttpPartnerEventsApiClient;

beforeAll(async () => {
  server = createServer(createMockPartnerApiApp());
  await new Promise<void>((resolve) => server.listen(0, resolve));
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error('failed to determine test server address');
  }
  baseUrl = `http://127.0.0.1:${address.port}`;
  client = new HttpPartnerEventsApiClient(baseUrl);
});

afterAll(() => {
  server.close();
});

describe('HttpPartnerEventsApiClient', () => {
  it('fetches the first page when no cursor is given', async () => {
    const page = await client.listEvents(null, 10);

    expect(page.events).toHaveLength(10);
    expect(page.events[0]).toEqual({
      eventId: 'partner_evt_0',
      paymentId: 'partner_pay_0',
      type: 'payment.failed',
    });
    expect(page.nextCursor).toBe('10');
  });

  it('fetches a subsequent page using the given cursor', async () => {
    const page = await client.listEvents('10', 10);

    expect(page.events[0].eventId).toBe('partner_evt_10');
    expect(page.events[0].paymentId).toBe('partner_pay_10');
    expect(page.nextCursor).toBe('20');
  });

  it('returns a partial page near the end of the list', async () => {
    const page = await client.listEvents('115', 10);

    expect(page.events).toHaveLength(5);
    expect(page.nextCursor).toBe('120');
  });
});
