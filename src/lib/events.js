import { EventEmitter } from 'events';

class FraudEventEmitter extends EventEmitter {}

const eventBus = new FraudEventEmitter();

export default eventBus;
