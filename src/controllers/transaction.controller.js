import TransactionService from '../services/transaction.service.js';

/**
 * TransactionController manages HTTP requests for transaction resources.
 */
class TransactionController {
    constructor(transactionService) {
        this.transactionService = transactionService || new TransactionService();
    }

    /**
     * Handles processing of a new transaction.
     * @param {import('express').Request} req 
     * @param {import('express').Response} res 
     * @param {import('express').NextFunction} next 
     */
    createTransaction = async (req, res, next) => {
        try {
            const result = await this.transactionService.createTransaction(req.body);
            res.status(201).json({ success: true, data: result });
        } catch (error) {
            next(error);
        }
    }

    /**
     * Set up a Server-Sent Events (SSE) connection for real-time updates.
     * @param {import('express').Request} req 
     * @param {import('express').Response} res 
     */
    streamEvents = (req, res) => {
        import('../lib/events.js').then(({ default: eventBus }) => {
            const accountId = req.params.accountId;

            res.setHeader('Content-Type', 'text/event-stream');
            res.setHeader('Cache-Control', 'no-cache');
            res.setHeader('Connection', 'keep-alive');
            res.flushHeaders();

            // Establish connection validation
            res.write(`data: ${JSON.stringify({ event: 'CONNECTED', accountId })}\n\n`);

            const handlePending = (data) => {
                if (data.account_id === accountId) res.write(`data: ${JSON.stringify({ event: 'PENDING', data })}\n\n`);
            };
            const handleCompleted = (data) => {
                if (data.account_id === accountId) res.write(`data: ${JSON.stringify({ event: 'COMPLETED', data })}\n\n`);
            };
            const handleBlocked = (data) => {
                if (data.account_id === accountId) res.write(`data: ${JSON.stringify({ event: 'BLOCKED', data })}\n\n`);
            };
            const handleFinalized = (data) => {
                // SAGA EVENT: Transaction fully verified across microservices
                res.write(`data: ${JSON.stringify({ event: 'FINALIZED', data })}\n\n`);
            };
            const handleCompensated = (data) => {
                if (data.account_id === accountId) res.write(`data: ${JSON.stringify({ event: 'COMPENSATED', data })}\n\n`);
            };

            eventBus.on('TransactionPending', handlePending);
            eventBus.on('TransactionCompleted', handleCompleted);
            eventBus.on('TransactionBlocked', handleBlocked);
            eventBus.on('TransactionFinalized', handleFinalized);
            eventBus.on('TransactionCompensated', handleCompensated);

            req.on('close', () => {
                eventBus.removeListener('TransactionPending', handlePending);
                eventBus.removeListener('TransactionCompleted', handleCompleted);
                eventBus.removeListener('TransactionBlocked', handleBlocked);
                eventBus.removeListener('TransactionFinalized', handleFinalized);
                eventBus.removeListener('TransactionCompensated', handleCompensated);
            });
        });
    }
}

export default TransactionController;
