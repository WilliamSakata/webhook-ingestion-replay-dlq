import { Router, Request, Response, NextFunction } from 'express';
import { ReconcilePayments } from '../../../application/use-cases/ReconcilePayments';

export function createReconciliationRouter(reconcilePayments: ReconcilePayments): Router {
  const router = Router();

  router.post('/reconciliation/run', async (_req: Request, res: Response, next: NextFunction) => {
    try {
      await reconcilePayments.execute();
      res.status(200).json({ status: 'completed' });
    } catch (error) {
      next(error);
    }
  });

  return router;
}
