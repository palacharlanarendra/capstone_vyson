import { Router } from 'express';
import TransactionController from '../controllers/transaction.controller.js';

const router = Router();
const transactionController = new TransactionController();

/**
 * Transaction Routes
 */

// Process a new transaction (Debit/Credit)
router.post('/', transactionController.createTransaction);

// Server-Sent Events stream for real-time transaction updates
router.get('/stream/:accountId', transactionController.streamEvents);

export default router;
