import AccountService from '../services/account.service.js';

/**
 * AccountController handles incoming HTTP requests and delegates to AccountService.
 */
class AccountController {
    constructor(accountService) {
        this.accountService = accountService || new AccountService();
    }

    /**
     * Handles account creation request.
     * @param {import('express').Request} req 
     * @param {import('express').Response} res 
     * @param {import('express').NextFunction} next 
     */
    createAccount = async (req, res, next) => {
        try {
            const newAccount = await this.accountService.createAccount(req.body);
            res.status(201).json({ success: true, data: newAccount });
        } catch (error) {
            next(error);
        }
    }
}

export default AccountController;
