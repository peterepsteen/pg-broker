import {getTableDefinitions} from './tables'
import { Pool } from 'pg';
import { Exchange } from './exchange';
import { QueueManager } from './queue';
import { Producer } from './producer';
import { Message } from './message';

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

    constructor(config?: PgBrokerSettings) {
        this.db = new Pool();
        this.config = Object.assign(configDefaults, config);
        this.producer = new Producer(this.db, this.config.schema);
        this.queueManager = new QueueManager(this.db, this.config.schema);
    }

    start = async () => {
        const createStmts = getTableDefinitions(this.config?.schema || configDefaults.schema);
        for (let stmt of createStmts) {
            await this.db.query(stmt);
        }
    }

    declareExchange = async (name: string) => {
        const id = await Exchange.assert(this.db, this.config.schema, name)
        return id;
    }

    declareQueue = async (name: string) => {
        //TODO: restrict queue naming
        const id = await this.queueManager.assert(name)
        return id;
    }

    produce = async (message: Message, exchangeName?: string, queueName?: string) => {
        return this.producer.produce(message, exchangeName, queueName);
    }

    subscribe = async (queueName: string, callback: (message: Message) => void) => {  
        await this.queueManager.subscribe(queueName, callback);
    }
};