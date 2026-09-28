import { Router, Request, Response, NextFunction } from 'express';
import { IngestWebhookEvent } from '../../../application/use-cases/IngestWebhookEvent';
import { InvalidPaymentEventError } from '../../../domain/services/PaymentEventValidator';

export function createWebhookRouter(ingestWebhookEvent: IngestWebhookEvent): Router {
  const router = Router();

  router.post('/webhooks/payments', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await ingestWebhookEvent.execute(req.body);
      res.status(202).json({ status: 'accepted' });
    } catch (error) {
      if (error instanceof InvalidPaymentEventError) {
        res.status(400).json({ error: 'invalid webhook payload', details: error.message });
        return;
      }
      next(error);
    }
  });

  return router;
}
