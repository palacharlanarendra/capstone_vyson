import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

class LedgerConsumer {
    constructor() {
        this.startListening();
    }

    startListening() {
        eventBus.on('TransactionCompleted', async (payload) => {
            if (payload.event_type !== 'TransactionCompleted') return;

            try {
                // Append-only ledger
                await prisma.ledgerEntry.create({
                    data: {
                        transactionId: payload.transaction_id,
                        accountId: payload.account_id,
                        amount: payload.amount,
                        balanceAfter: payload.balance_after
                    }
                });
                console.log(`[LEDGER SERVICE] 📝 Successfully appended exact-once entry for Transaction ${payload.transaction_id}`);
                
                // Emit event for Saga Orchestrator
                eventBus.emit('LedgerUpdated', { transaction_id: payload.transaction_id, account_id: payload.account_id });
            } catch (error) {
                // Handle duplicate gracefully due to idempotency
                if (error.code === 'P2002') {
                    console.log(`[LEDGER SERVICE] ⚠️ Ledger entry already exists for Transaction ${payload.transaction_id} (Idempotent)`);
                } else {
                    console.error(`[LEDGER SERVICE ERROR] Failed to append ledger entry:`, error);
                }
            }
        });
    }
}

export default new LedgerConsumer();
