import eventBus from '../lib/events.js';

class NotificationConsumer {
    constructor() {
        this.startListening();
    }

    startListening() {
        eventBus.on('TransactionCompleted', (payload) => {
            if (payload.event_type !== 'TransactionCompleted') return;
            
            console.log(`[NOTIFICATION SERVICE] 📧 Sending SMS/Email to user for Account ${payload.account_id}. Amount: ₹${payload.amount}. New Balance: ₹${payload.balance_after}. Transaction ID: ${payload.transaction_id}`);
        });
    }
}

export default new NotificationConsumer();
