import { Router, Request, Response, NextFunction } from 'express';
import { DlqRepositoryPort } from '../../../application/ports/DlqRepositoryPort';
import { ReprocessDlqEvent, DlqEntryNotFoundError } from '../../../application/use-cases/ReprocessDlqEvent';

export function createDlqRouter(dlqRepository: DlqRepositoryPort, reprocessDlqEvent: ReprocessDlqEvent): Router {
  const router = Router();

  router.get('/dlq', async (req: Request, res: Response, next: NextFunction) => {
    try {
      const limit = typeof req.query.limit === 'string' ? Number(req.query.limit) : 20;
      const offset = typeof req.query.offset === 'string' ? Number(req.query.offset) : 0;
      const entries = await dlqRepository.list({ limit, offset });
      res.status(200).json({ entries });
    } catch (error) {
      next(error);
    }
  });

  router.post('/dlq/:id/reprocess', async (req: Request, res: Response, next: NextFunction) => {
    try {
      await reprocessDlqEvent.execute(req.params.id);
      res.status(200).json({ status: 'reprocessed' });
    } catch (error) {
      if (error instanceof DlqEntryNotFoundError) {
        res.status(404).json({ error: 'DLQ entry not found' });
        return;
      }
      next(error);
    }
  });

  return router;
}
