import prisma from '../lib/prisma.js';

class AuditService {
    /**
     * Trace an entire event correlation path to answer regulatory queries "Why was X debited?"
     * @param {string} correlationId - The transaction ID
     */
    async traceEventPath(correlationId) {
        const events = await prisma.eventStore.findMany({
            where: { correlationId },
            orderBy: { createdAt: 'asc' }
        });

        console.log(`\n=== 🔎 AUDIT TRACE FOR [${correlationId}] ===`);
        events.forEach(e => {
            console.log(`[${e.createdAt.toISOString()}] ${e.eventType}: ${e.payload}`);
        });
        console.log(`=================================================\n`);

        return events;
    }

    /**
     * Rebuild absolute account balance deterministically purely from historical events
     * @param {string} accountId 
     */
    async rebuildBalanceFromEvents(accountId) {
        // Collect all immutable events for this specific account
        const events = await prisma.eventStore.findMany({
            where: { aggregateId: accountId },
            orderBy: { createdAt: 'asc' }
        });

        let rebuiltBalance = 0;
        
        // 1. First find if there's an AccountCreated event (if we emitted one)
        // 2. Iterate dynamically over final Transaction events
        // 3. Subtract DEBIT, Add CREDIT.
        
        // Filter out deduplicated successful transactions 
        // We only want to apply effects for `TransactionCompleted` signals,
        // and safely inverse `TransactionCompensated` if they exist.
        
        for (const event of events) {
            const payload = JSON.parse(event.payload);

            if (event.eventType === 'TransactionCompleted') {
                if (payload.type === 'DEBIT') {
                    rebuiltBalance -= payload.amount;
                } else if (payload.type === 'CREDIT' || payload.type === 'DEPOSIT') {
                    rebuiltBalance += payload.amount;
                }
            }
            
            // Apply reversals precisely
            if (event.eventType === 'TransactionCompensated') {
                // Find the original to reverse the math
                const originalTx = events.find(e => e.eventType === 'TransactionCompleted' && e.correlationId === event.correlationId);
                if (originalTx) {
                    const originalPayload = JSON.parse(originalTx.payload);
                    if (originalPayload.type === 'DEBIT') {
                        rebuiltBalance += originalPayload.amount; // Give it back
                    } else if (originalPayload.type === 'CREDIT' || originalPayload.type === 'DEPOSIT') {
                        rebuiltBalance -= originalPayload.amount; // Take it back
                    }
                }
            }
        }
        
        console.log(`\n=== 📈 EVENT SOURCED REPLAY BUILD FOR ${accountId} ===`);
        console.log(`Replayed over ${events.length} aggregate events.`);
        console.log(`Calculated Deterministic Absolute Balance: ₹${rebuiltBalance}`);
        console.log(`==========================================================\n`);

        return rebuiltBalance;
    }
}

export default new AuditService();
