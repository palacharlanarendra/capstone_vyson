import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';
import AccountService from './account.service.js';

class SagaOrchestrator {
    constructor() {
        this.accountService = new AccountService();
        this.startListening();
        
        // Start saga timeout monitor (run every 10s)
        setInterval(() => this.monitorTimeouts(), 10000);
    }

    startListening() {
        eventBus.on('LedgerUpdated', async (payload) => {
            const { transaction_id } = payload;
            await this.handleLedgerUpdated(transaction_id);
        });
    }

    async handleLedgerUpdated(transactionId) {
        try {
            // Read aggregate state
            const saga = await prisma.transactionOrchestrator.findUnique({
                where: { transactionId }
            });

            if (!saga || saga.status !== 'PENDING') return;

            // Maintain aggregate state: Ledger updated = true
            const updatedSaga = await prisma.transactionOrchestrator.update({
                where: { transactionId },
                data: { ledgerUpdated: true }
            });

            // Check completion conditions
            if (updatedSaga.paymentAuth && updatedSaga.ledgerUpdated) {
                await this.finalizeTransaction(transactionId);
            }
        } catch (error) {
            console.error(`[SAGA ERROR] Failed to handle LedgerUpdated for ${transactionId}:`, error);
        }
    }

    async finalizeTransaction(transactionId) {
        try {
            await prisma.$transaction(async (tx) => {
                await tx.transactionOrchestrator.update({
                    where: { transactionId },
                    data: { status: 'FINALIZED' }
                });

                await tx.transaction.update({
                    where: { id: transactionId },
                    data: { status: 'FINALIZED' }
                });
            });

            console.log(`[SAGA SUCCESS] Transaction ${transactionId} is now FINALIZED.`);
            
            // Trigger Finalized event for SSE or others
            eventBus.emit('TransactionFinalized', { transaction_id: transactionId });
        } catch (error) {
            console.error(`[SAGA ERROR] Failed to finalize transaction ${transactionId}:`, error);
        }
    }

    async monitorTimeouts() {
        try {
            // Find PENDING orchestrator tasks older than 30 seconds
            const thirtySecsAgo = new Date(Date.now() - 30 * 1000);
            const stuckSagas = await prisma.transactionOrchestrator.findMany({
                where: {
                    status: 'PENDING',
                    createdAt: { lte: thirtySecsAgo }
                },
                include: { transaction: true }
            });

            for (const saga of stuckSagas) {
                await this.compensateTransaction(saga);
            }
        } catch (error) {
            console.error(`[SAGA MONITOR ERROR]`, error);
        }
    }

    async compensateTransaction(saga) {
        console.log(`[SAGA TIMEOUT] Transaction ${saga.transactionId} timed out waiting for Ledger. Initiating compensation...`);
        
        try {
            await prisma.$transaction(async (tx) => {
                // Determine reversal direction (if DEBIT was made, credit it back)
                const reverseAmount = saga.transaction.type === 'DEBIT' 
                    ? saga.transaction.amount 
                    : -saga.transaction.amount;

                // Revert account balance
                await this.accountService.updateBalance(saga.transaction.accountId, reverseAmount, tx);

                // Update models to Failed/Compensated
                await tx.transaction.update({
                    where: { id: saga.transactionId },
                    data: { status: 'FAILED' }
                });

                await tx.transactionOrchestrator.update({
                    where: { id: saga.id },
                    data: { status: 'COMPENSATED' }
                });
            });

            console.log(`[SAGA RECOVERY] Successfully compensated and reversed Transaction ${saga.transactionId}`);
            
            // Announce Compensation Event
            eventBus.emit('TransactionCompensated', { transaction_id: saga.transactionId, account_id: saga.transaction.accountId });

        } catch (error) {
            console.error(`[SAGA FATAL] Failed to compensate transaction ${saga.transactionId}:`, error);
        }
    }
}

export default new SagaOrchestrator();
