import { getKafka } from '../lib/kafka.js';

class KafkaAuditService {
    /**
     * Reads historical data synchronously from the topic for replay
     */
    async fetchAllEvents(topic, filterPredicate = () => true) {
        const kafka = getKafka();
        const consumer = kafka.consumer({ groupId: `audit-query-${Date.now()}` });
        
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });
        
        const results = [];
        let consuming = true;
        
        return new Promise(async (resolve) => {
            // Because standard Kafka flows are infinite, this query sets an auto-finish timeout when the local offset cache reaches HEAD.
            let timeout;
            const finish = async () => {
                if (!consuming) return;
                consuming = false;
                await consumer.disconnect();
                
                // Sort ascending by Kafka log time to guarantee chronological determinism
                results.sort((a,b) => parseInt(a.timestamp) - parseInt(b.timestamp));
                resolve(results);
            };

            await consumer.run({
                eachMessage: async ({ message }) => {
                    clearTimeout(timeout);
                    try {
                        const eventData = JSON.parse(message.value.toString());
                        const eventType = message.headers?.eventType?.toString();
                        
                        const fullEvent = {
                            correlationId: message.key.toString(),
                            eventType,
                            payload: eventData,
                            timestamp: message.timestamp
                        };
                        
                        if (filterPredicate(fullEvent)) {
                            results.push(fullEvent);
                        }
                    } catch (e) {
                         // silently ignore parsing errors from alien producers
                    }
                    // Extend tail timeout every time we see activity
                    timeout = setTimeout(finish, 400);
                }
            });
            // Initial boot timeout
            timeout = setTimeout(finish, 1500);
        });
    }

    /**
     * Trace an entire event correlation path using Kafka logs
     * @param {string} correlationId
     */
    async traceEventPath(correlationId) {
        console.log(`\n=== 🔎 [KAFKA] K-STREAM AUDIT TRACE FOR [${correlationId}] ===`);
        const events = await this.fetchAllEvents('banking-events', e => e.correlationId === correlationId);

        events.forEach(e => {
            console.log(`[${new Date(parseInt(e.timestamp)).toISOString()}] ${e.eventType}: ${JSON.stringify(e.payload)}`);
        });
        console.log(`=================================================================\n`);
        return events;
    }

    /**
     * Rebuild absolute account balance deterministically from historical Kafka streams
     * @param {string} accountId 
     */
    async rebuildBalanceFromEvents(accountId) {
        console.log(`\n=== 📈 [KAFKA] EVENT SOURCED CONSUMER REPLAY FOR ${accountId} ===`);
        
        const events = await this.fetchAllEvents('banking-events', e => {
            return e.payload.account_id === accountId || e.payload.accountId === accountId;
        });

        let rebuiltBalance = 0;
        
        for (const event of events) {
            const payload = event.payload;

            if (event.eventType === 'TransactionCompleted') {
                if (payload.type === 'DEBIT') {
                    rebuiltBalance -= payload.amount;
                } else if (payload.type === 'CREDIT' || payload.type === 'DEPOSIT') {
                    rebuiltBalance += payload.amount;
                }
            }
            
            if (event.eventType === 'TransactionCompensated') {
                const originalTx = events.find(e => e.eventType === 'TransactionCompleted' && e.correlationId === event.correlationId);
                if (originalTx) {
                    const originalPayload = originalTx.payload;
                    if (originalPayload.type === 'DEBIT') rebuiltBalance += originalPayload.amount;
                    else if (originalPayload.type === 'CREDIT' || originalPayload.type === 'DEPOSIT') rebuiltBalance -= originalPayload.amount;
                }
            }
        }
        
        console.log(`Streamed ${events.length} aggregate events from 'banking-events' topic`);
        console.log(`Calculated Deterministic Absolute Balance: ₹${rebuiltBalance}`);
        console.log(`===================================================================\n`);

        return rebuiltBalance;
    }
}

export default new KafkaAuditService();
