import { Kafka } from 'kafkajs';

const kafka = new Kafka({
  clientId: 'banking-app',
  brokers: ['localhost:9092'],
  retry: {
    initialRetryTime: 100,
    retries: 8
  }
});

const producer = kafka.producer();

export const connectKafka = async () => {
    try {
        await producer.connect();
        console.log('[KAFKA] Connected to broker successfully');
    } catch (err) {
        console.error('[KAFKA ERROR] Failed to connect:', err);
    }
};

export const getProducer = () => producer;
export const getKafka = () => kafka;
