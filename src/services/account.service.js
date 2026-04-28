import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

/**
 * AccountService handles the core business logic for user accounts.
 */
class AccountService {
    constructor() {
        this.prisma = prisma;
    }

    /**
     * Creates a new account.
     * @param {Object} accountData - The data for the new account.
     * @returns {Promise<Object>} The created account.
     */
    async createAccount(accountData) {
        const { id, initialBalance = 0 } = accountData;
        if (!id) {
            const error = new Error('Account ID is required.');
            error.status = 400;
            throw error;
        }
        const account = await this.prisma.account.create({
            data: {
                id,
                balance: initialBalance
            }
        });

        if (initialBalance > 0) {
            // Treat initial balances directly as deposit events for Event Sourcing replays
            eventBus.emit('TransactionCompleted', {
                transaction_id: `init_${id}`,
                account_id: id,
                type: 'DEPOSIT',
                amount: initialBalance,
                balance_after: initialBalance
            });
        }
        
        return account;
    }

    /**
     * Updates an account balance atomically.
     * @param {String} accountId - The ID of the account
     * @param {Number} balanceChange - The amount to increment or decrement (+/-)
     * @param {Object} tx - Optional Prisma transaction client
     * @returns {Promise<Object>} The updated account
     */
    async updateBalance(accountId, balanceChange, tx = null) {
        // Use the injected transaction client if provided, otherwise fallback to main client
        const db = tx || this.prisma;
        return await db.account.update({
            where: { id: accountId },
            data: {
                balance: {
                    increment: balanceChange
                }
            }
        });
    }
}

export default AccountService;
