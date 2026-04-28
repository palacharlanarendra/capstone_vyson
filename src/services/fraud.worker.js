import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

class FraudWorker {
    constructor() {
        this.startListening();
    }

    startListening() {
        eventBus.on('TransactionCompleted', async (payload) => {
            await this.processAsyncFraudRules(payload);
        });

        eventBus.on('TransactionBlocked', (payload) => {
            console.log(`[ALERT] Transaction ${payload.transaction_id} was BLOCKED synchronously. Reason: ${payload.reason}`);
        });
    }

    async processAsyncFraudRules(payload) {
        const { transaction_id, account_id, ip_address, device_fingerprint } = payload;
        
        try {
            // Idempotency: Check if this analysis already exists
            const existingAnalysis = await prisma.fraudAnalysis.findUnique({
                where: { transactionId: transaction_id }
            });

            if (existingAnalysis) {
                return; // Already processed or processing
            }

            // Create initial state
            const analysis = await prisma.fraudAnalysis.create({
                data: {
                    transactionId: transaction_id,
                    status: 'PENDING'
                }
            });

            console.log(`[ASYNC FRAUD] Analyzing transaction ${transaction_id} for suspicious patterns...`);

            let isSuspicious = false;
            let reason = null;

            // Pattern checking based on device & IP
            if (device_fingerprint && device_fingerprint.includes('bot')) {
                isSuspicious = true;
                reason = 'Bot-like device fingerprint detected';
            } 
            if (ip_address && ip_address === '0.0.0.0') {
                isSuspicious = true;
                reason = 'Blacklisted IP address detected';
            }

            if (isSuspicious) {
                console.log(`[ASYNC FRAUD] 🚩 Suspicious activity detected! Flagging account ${account_id}. Reason: ${reason}`);
                
                // Flag the account (soft rule consequence)
                await prisma.$transaction([
                    prisma.fraudAnalysis.update({
                        where: { id: analysis.id },
                        data: { status: 'ANALYZED', reason: reason }
                    }),
                    prisma.account.update({
                        where: { id: account_id },
                        data: { isFlagged: true }
                    })
                ]);
            } else {
                await prisma.fraudAnalysis.update({
                    where: { id: analysis.id },
                    data: { status: 'ANALYZED', reason: 'Clean' }
                });
            }

        } catch (error) {
            console.error(`[ASYNC FRAUD ERRROR] Failed to process async fraud rules for transaction ${transaction_id}:`, error.message);
            // It is retryable: The status remains 'PENDING' or doesn't exist, capable of being picked up again
        }
    }
}

// Instantiate to attach event listeners
export default new FraudWorker();
