import {getTableDefinitions} from './tables'
import { Pool } from 'pg';
import { ExchangeManager } from './exchange';
import { QueueManager } from './queue';
import { Producer } from './producer';
import { Consumer } from './consumer';

interface PgBrokerSettings {
    schema: string
}

const configDefaults: PgBrokerSettings = {
    schema: 'pgbroker'
}

export class PgBroker {
    private db: Pool;
    private config: PgBrokerSettings;
    private producer: Producer;
    private queueManager: QueueManager;
    private exchangeManager: ExchangeManager;

    constructor(config?: PgBrokerSettings) {
        this.db = new Pool();
        this.config = Object.assign(configDefaults, config);
        this.exchangeManager = new ExchangeManager(this.db, this.config.schema);
        this.producer = new Producer(this.db, this.config.schema, this.exchangeManager);
        this.queueManager = new QueueManager(this.db, this.config.schema, this.exchangeManager);
    }

    start = async () => {
        const createStmts = getTableDefinitions(this.config?.schema || configDefaults.schema);
        for (let stmt of createStmts) {
            await this.db.query(stmt);
        }
    }

    declareExchange = async (name: string) => {
        console.log('DEBUG creating exchange', {name});
        const id = await this.exchangeManager.assert(name)
        console.log('DEBUG created exchange', {name, id});
        return id;
    }

    declareQueue = async (name: string) => {
        //TODO: restrict queue naming
        const id = await this.queueManager.assert(name)
        return id;
    }

    produce = async <T>(message: T, exchangeName?: string, queueName?: string) => {
        return this.producer.produce<T>(message, exchangeName, queueName);
    }

    subscribe = async <T>(queueName: string, callback: (message: T) => void): Promise<Consumer> => {  
        const consumer = new Consumer(this.db, this.config.schema, this.queueManager);

        await consumer.subscribe(queueName, callback);

        return consumer;
    }

    bindQueueToExchange = async (queueName: string, exchangeName: string) => {
        await this.queueManager.bindQueueToExchange(queueName, exchangeName);
    }
};