import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

class AuditConsumer {
    constructor() {
        this.startListening();
    }

    startListening() {
        const eventsToTrack = [
            'TransactionPending',
            'TransactionCompleted',
            'TransactionBlocked',
            'TransactionFinalized',
            'TransactionCompensated',
            'LedgerUpdated'
        ];

        for (const eventName of eventsToTrack) {
            eventBus.on(eventName, async (payload) => {
                await this.storeEvent(eventName, payload);
            });
        }
    }

    async storeEvent(eventType, payload) {
        try {
            // Correlation IDs link events back to specific transaction workflows
            const correlationId = payload.transaction_id || payload.transactionId || 'UNKNOWN';
            const aggregateId = payload.account_id || payload.accountId || 'UNKNOWN';

            // Event Store Append-Only Persistance
            await prisma.eventStore.create({
                data: {
                    aggregateId,
                    correlationId,
                    eventType,
                    payload: JSON.stringify(payload)
                }
            });
            console.log(`[AUDIT STORE] 🗄️ Persisted ${eventType} for correlation_id: ${correlationId}`);
        } catch (error) {
            console.error(`[AUDIT STORE ERROR] Failed to save ${eventType}:`, error);
        }
    }
}

export default new AuditConsumer();
