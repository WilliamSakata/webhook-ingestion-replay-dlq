import express, { Express } from 'express';

interface SyntheticEvent {
  eventId: string;
  paymentId: string;
  type: 'payment.succeeded' | 'payment.failed';
}

function generateEvents(count: number): SyntheticEvent[] {
  const events: SyntheticEvent[] = [];
  for (let i = 0; i < count; i += 1) {
    events.push({
      eventId: `partner_evt_${i}`,
      paymentId: `partner_pay_${i}`,
      type: i % 5 === 0 ? 'payment.failed' : 'payment.succeeded',
    });
  }
  return events;
}

const ALL_EVENTS = generateEvents(120);

export function createMockPartnerApiApp(): Express {
  const app = express();

  app.get('/events', (req, res) => {
    const cursor = typeof req.query.cursor === 'string' ? Number(req.query.cursor) : 0;
    const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 50;
    const start = Number.isNaN(cursor) ? 0 : cursor;
    const page = ALL_EVENTS.slice(start, start + limit);
    const nextCursor = String(start + page.length);

    res.status(200).json({ events: page, nextCursor });
  });

  return app;
}

if (require.main === module) {
  const app = createMockPartnerApiApp();
  const port = Number(process.env.PORT ?? 4002);
  app.listen(port, () => {
    console.log(`mock-partner-api listening on port ${port}`);
  });
}
