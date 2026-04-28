import prisma from '../lib/prisma.js';
import AccountService from './account.service.js';
import eventBus from '../lib/events.js';

/**
 * TransactionService handles business logic related to transactions.
 */
class TransactionService {
    constructor() {
        this.prisma = prisma;
        this.accountService = new AccountService();
    }

    /**
     * Creates a new transaction (e.g., DEBIT or CREDIT).
     * @param {Object} transactionData - Details of the transaction.
     * @returns {Promise<Object>} The created transaction.
     */
    async createTransaction(transactionData) {
        const { transaction_id, account_id, type, amount, ip_address, device_fingerprint } = transactionData;

        // Emit state immediately before processing validations
        eventBus.emit('TransactionPending', { transaction_id, account_id, type, amount });

        // 1. Immediate Fraud Blocking & Validation
        if (!amount || amount <= 0) {
            const error = new Error('Amount must be strictly positive.');
            error.status = 400;
            throw error;
        }

        // Rule: Amount > ₹50,000 (Hard stop)
        if (amount > 50000) {
            eventBus.emit('TransactionBlocked', { transaction_id, account_id, reason: 'Transaction amount > ₹50,000' });
            const error = new Error('Fraud block: Transaction amount exceeds safety limit.');
            error.status = 403;
            throw error;
        }

        // Rule: Velocity limit > 5 txns in last 60 seconds (Hard stop)
        const sixtySecondsAgo = new Date(Date.now() - 60 * 1000);
        const recentTxnCount = await this.prisma.transaction.count({
            where: {
                accountId: account_id,
                createdAt: { gte: sixtySecondsAgo }
            }
        });

        if (recentTxnCount >= 5) {
            eventBus.emit('TransactionBlocked', { transaction_id, account_id, reason: 'High transaction velocity (5+ per min)' });
            const error = new Error('Fraud block: Too many transactions in a short period.');
            error.status = 429;
            throw error;
        }
        if (type !== 'DEBIT' && type !== 'CREDIT') {
            const error = new Error('Transaction type must be DEBIT or CREDIT.');
            error.status = 400;
            throw error;
        }

        // 2 & 5. Atomic DB Transaction with exactly-once guarantees
        return await this.prisma.$transaction(async (tx) => {
            // 3. Idempotency using transaction_id
            const existingTxn = await tx.transaction.findUnique({
                where: { id: transaction_id }
            });
            
            if (existingTxn) {
                // If it already exists, gracefully return the previous success (Idempotent)
                return existingTxn;
            }

            const isDebit = type === 'DEBIT';
            const balanceChange = isDebit ? -amount : amount;

            // 4. Row-level locking equivalent (Atomic Increment)
            // Delegated to AccountService to keep concerns cleanly separated
            const updatedAccount = await this.accountService.updateBalance(account_id, balanceChange, tx);

            // Prevent overdrafts on Debits
            if (isDebit && updatedAccount.balance < 0) {
                const error = new Error('Insufficient funds. Transaction rolled back.');
                error.status = 400;
                throw error;
            }

            // Exactly-once transaction record creation
            const newTransaction = await tx.transaction.create({
                data: {
                    id: transaction_id,
                    accountId: account_id,
                    type: type,
                    amount: amount,
                    status: 'AUTHORIZED', // Changed to AUTHORIZED for Orchestration flow
                    ipAddress: ip_address,
                    deviceFingerprint: device_fingerprint
                }
            });

            // Orchestration / Saga Initialization
            await tx.transactionOrchestrator.create({
                data: {
                    transactionId: transaction_id,
                    paymentAuth: true,
                    ledgerUpdated: false,
                    status: 'PENDING'
                }
            });

            // At-least-once: Secure the transaction event using the DB Outbox Pattern
            const eventPayload = {
                event_type: 'TransactionCompleted',
                event_version: 1,
                transaction_id,
                account_id,
                type,
                amount,
                balance_after: updatedAccount.balance,
                ip_address,
                device_fingerprint
            };

            await tx.outboxEvent.create({
                data: {
                    eventType: 'TransactionCompleted',
                    payload: JSON.stringify(eventPayload),
                    status: 'PENDING'
                }
            });

            return newTransaction;
        });
    }
}

export default TransactionService;
