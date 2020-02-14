import { Pool } from 'pg';
import uuid = require('uuid');
import { QueueManager } from '../queue';

const getNextMessageSQL = (schema: string) => `
    DELETE FROM ${schema}.message_queue
        WHERE message_id = (
            SELECT message_id FROM ${schema}.message_queue
            WHERE queue_id = $1
            ORDER BY created_at 
            FOR UPDATE SKIP LOCKED 
            LIMIT 1
        ) AND queue_id = $1
    RETURNING queue_id, message_id, message;
`

export class Consumer {
    id: string;
    constructor(
        private pool: Pool, 
        private schema: string, 
        private queueManager: QueueManager
    ){
        this.id = uuid();
    }

    private tryGetMessage = async <T>(queueId: string, callback: (message: T) => void): Promise<Boolean> => {
        console.log(`DEBUG ${this.id} trying to get next message`, {queueId});

        const res = await this.pool.query(getNextMessageSQL(this.schema), [queueId]);
        if (!res.rowCount) {
            console.log(`DEBUG ${this.id} no message available`, {queueId});
            return false;
        }
        
        console.log(`DEBUG ${this.id} got message`, {queueId});
        try {
            const {message} = res.rows[0];
            const typedMessage = JSON.parse(message) as T
            callback(typedMessage);   
        } catch(error) {
            console.error(`${this.id} error parsing message`, {error});
            throw error;
        }
        
        return true;
    }

    subscribe = async <T>(queueName: string, callback: (message: T) => void) => {
        console.log('DEBUG subscribing to queue', {queueName});
        const id = await this.queueManager.getQueueId(queueName);

        //initially, see if anything is currently queued
        while (await this.tryGetMessage(id, callback)){}

        // aquire a client, then listen on the queues channel. Resolve
        // so the subscriber can continue, and handle the notification by
        // trying to get a message in the queue
        return new Promise(resolve => this.pool.connect(async (err, client, done) => {
            await client.query(`LISTEN ${queueName}_channel`);
            resolve();

            client.on('notification', msg => {
                const queueId = msg.payload as string;
                console.log(`DEBUG ${this.id} received notification`, {queueId, queueName})
                this.tryGetMessage(queueId, callback);
            });
        }));
    }
}