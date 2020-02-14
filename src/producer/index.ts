import { Pool } from 'pg';
import { Message } from '../message';
import uuid = require('uuid');

const insertToBoundQueuesSql = (schema: string) => `
    INSERT INTO ${schema}.message_queue (
        queue_id, 
        message_id,
        message
    ) 
    SELECT queue_id, $1, $2
    FROM ${schema}.queue
    WHERE queue_name = $3
    RETURNING message_id
`;

export class Producer {
    constructor(private pool: Pool, private schema: string){}

    private toExchange = (message: Message, exchange: string) => {
        // get all queues currently bound to queue

        // create entries in queue_message
    }

    private toQueue = async (message: Message, queueName: string) => {
        const messageText = JSON.stringify(message.message);
        const res = await this.pool.query(
            insertToBoundQueuesSql(this.schema), 
            [uuid(), messageText, queueName]
        );

        if (!res.rowCount) {
            console.warn(`Queue ${queueName} does not seem to exist, message dropped`);
            return '';
        }

        return res.rows[0].message_id
    }

    produce = (message: Message, exchangeName?: string, queueName?: string) => {
        if (exchangeName && queueName) {
            console.warn(`Producing messages for both a specific exhange and queue not supported. Using exchange ${exchangeName}`);
            queueName = '';
        }

        if (exchangeName) {
            return this.toExchange(message, exchangeName)
        }

        if (!queueName) {
            throw new Error('One of exchangeName or queueName must be provided');
        }

        return this.toQueue(message, queueName);
    }
}