import prisma from '../lib/prisma.js';
import eventBus from '../lib/events.js';

const MAX_RETRIES = 3;

class OutboxWorker {
    constructor() {
        this.pollInterval = 5000; // Poll every 5 seconds
        this.isPolling = false;
        
        // Start polling gracefully
        setTimeout(() => {
            setInterval(() => this.poll(), this.pollInterval);
            this.poll(); // Trigger initial poll
        }, 1000);
    }

    async poll() {
        if (this.isPolling) return;
        this.isPolling = true;

        try {
            // Find events that are PENDING or (FAILED and nextRetryAt <= NOW)
            const events = await prisma.outboxEvent.findMany({
                where: {
                    OR: [
                        { status: 'PENDING' },
                        { status: 'FAILED', nextRetryAt: { lte: new Date() } }
                    ]
                },
                take: 10,
                orderBy: { createdAt: 'asc' }
            });

            for (const event of events) {
                await this.processEvent(event);
            }
        } catch (error) {
            console.error(`[OUTBOX WORKER ERROR] Polling failed:`, error);
        } finally {
            this.isPolling = false;
        }
    }

    async processEvent(event) {
        // Lock the event (Worker crash mid-task safety)
        try {
            await prisma.outboxEvent.update({
                where: { id: event.id, status: event.status }, // Optimistic locking
                data: { status: 'PROCESSING' }
            });
        } catch (e) {
            return; // Already locked or processed by another instance
        }

        try {
            // Parse payload
            const payload = JSON.parse(event.payload);

            // Poison message simulation: e.g. payload.amount is simulated poison
            if (payload.amount === "POISON") {
                throw new Error("Simulated poison message corrupted payload");
            }

            // At-least-once delivery: Broadcast to internal consumers via eventBus
            eventBus.emit(event.eventType, payload);

            // Mark COMPLETED
            await prisma.outboxEvent.update({
                where: { id: event.id },
                data: { status: 'COMPLETED' }
            });

        } catch (error) {
            console.error(`[OUTBOX EVENT ERROR] Event ${event.id} failed:`, error.message);
            
            const newRetryCount = event.retryCount + 1;
            
            if (newRetryCount >= MAX_RETRIES) {
                // Move to Dead Letter Queue
                await prisma.$transaction([
                    prisma.deadLetterQueue.create({
                        data: {
                            eventId: event.id,
                            eventType: event.eventType,
                            payload: event.payload,
                            failureReason: error.message
                        }
                    }),
                    prisma.outboxEvent.update({
                        where: { id: event.id },
                        data: { status: 'DLQ', errorReason: error.message }
                    })
                ]);
                console.log(`[DLQ] Event ${event.id} moved to Dead Letter Queue after ${newRetryCount} failures.`);
            } else {
                // Retry with exponential backoff (2s, 4s, 8s, etc)
                const backoffSeconds = Math.pow(2, newRetryCount);
                const nextRetryAt = new Date(Date.now() + backoffSeconds * 1000);

                await prisma.outboxEvent.update({
                    where: { id: event.id },
                    data: { 
                        status: 'FAILED',
                        retryCount: newRetryCount,
                        nextRetryAt,
                        errorReason: error.message
                    }
                });
                console.log(`[OUTBOX RETRY] Event ${event.id} scheduled for retry at ${nextRetryAt.toISOString()}`);
            }
        }
    }

    // Manual reprocessing from DLQ
    async requeueFromDLQ(eventId) {
        const dlqEntry = await prisma.deadLetterQueue.findUnique({ where: { eventId } });
        if (!dlqEntry) throw new Error("DLQ entry not found");

        await prisma.$transaction([
            prisma.deadLetterQueue.delete({ where: { eventId } }),
            prisma.outboxEvent.update({
                where: { id: eventId },
                data: { status: 'PENDING', retryCount: 0, errorReason: null, nextRetryAt: new Date() }
            })
        ]);
        console.log(`[DLQ REQUEUE] Event ${eventId} manually moved back to PENDING state.`);
        this.poll(); // Trigger immediate processing
    }
}

export default new OutboxWorker();
