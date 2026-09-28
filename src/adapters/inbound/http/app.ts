import express, { Express, Request, Response, NextFunction } from 'express';
import { IngestWebhookEvent } from '../../../application/use-cases/IngestWebhookEvent';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent } from '../../../application/use-cases/ReprocessDlqEvent';
import { ReconcilePayments } from '../../../application/use-cases/ReconcilePayments';
import { createWebhookRouter } from './webhookRouter';
import { createDlqRouter } from './dlqRouter';
import { createReconciliationRouter } from './reconciliationRouter';

export interface AppDependencies {
  ingestWebhookEvent: IngestWebhookEvent;
  dlqRepository: DlqRepositoryPort;
  reprocessDlqEvent: ReprocessDlqEvent;
  reconcilePayments: ReconcilePayments;
}

export function createApp(deps: AppDependencies): Express {
  const app = express();
  app.use(express.json());
  app.use(createWebhookRouter(deps.ingestWebhookEvent));
  app.use(createDlqRouter(deps.dlqRepository, deps.reprocessDlqEvent));
  app.use(createReconciliationRouter(deps.reconcilePayments));

  app.use((err: unknown, _req: Request, res: Response, _next: NextFunction) => {
    if (err instanceof SyntaxError && 'status' in err && (err as { status?: number }).status === 400) {
      res.status(400).json({ error: 'invalid JSON body' });
      return;
    }
    res.status(500).json({ error: 'internal server error' });
  });

  return app;
}
