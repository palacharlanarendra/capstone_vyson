import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

class AnalyticsConsumer {
    constructor() {
        this.startListening();
    }

    startListening() {
        eventBus.on('TransactionCompleted', async (payload) => {
            if (payload.event_type !== 'TransactionCompleted') return;

            try {
                // Get today's date formatted as YYYY-MM-DD
                const today = new Date().toISOString().split('T')[0];

                console.log(`[ANALYTICS SERVICE] 📊 Aggregating metrics for ${today}...`);

                await prisma.dailyMetrics.upsert({
                    where: { date: today },
                    update: {
                        totalTxnCount: { increment: 1 },
                        totalVolume: { increment: payload.amount }
                    },
                    create: {
                        date: today,
                        totalTxnCount: 1,
                        totalVolume: payload.amount
                    }
                });
                console.log(`[ANALYTICS SERVICE] 📊 Successfully aggregated metrics for ${today}`);

            } catch (error) {
                console.error(`[ANALYTICS SERVICE ERROR] Failed to aggregate metrics:`, error);
            }
        });
    }
}

export default new AnalyticsConsumer();
