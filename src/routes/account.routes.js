import { Router } from 'express';
import AccountController from '../controllers/account.controller.js';

const router = Router();
const accountController = new AccountController();

/**
 * Account Routes
 */

// Create a new account
router.post('/', accountController.createAccount);

export default router;
