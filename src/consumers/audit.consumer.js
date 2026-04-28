import { getProducer } from '../lib/kafka.js';
import eventBus from '../lib/events.js';

class AuditKafkaProducer {
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
            'LedgerUpdated',
            'AccountCreated'
        ];

        for (const eventName of eventsToTrack) {
            eventBus.on(eventName, async (payload) => {
                await this.publishToKafka(eventName, payload);
            });
        }
    }

    async publishToKafka(eventType, payload) {
        try {
            const correlationId = payload.transaction_id || payload.transactionId || 'UNKNOWN';
            const producer = getProducer();

            await producer.send({
                topic: 'banking-events',
                messages: [
                    {
                        key: correlationId, // Partition mapping for sequentially ordered traces
                        headers: {
                            eventType: eventType
                        },
                        value: JSON.stringify(payload)
                    }
                ]
            });
            console.log(`[KAFKA AUDIT] Published ${eventType} into Event Store for trace ${correlationId}`);
        } catch (error) {
            console.error(`[KAFKA ERROR] Failed to push trace to Kafka for ${eventType}:`, error);
        }
    }
}

export default new AuditKafkaProducer();
