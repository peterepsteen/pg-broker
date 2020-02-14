import { Pool } from 'pg';
import { Message } from '../message';
import uuid = require('uuid');
import { ExchangeManager } from '../exchange';

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
    constructor(private pool: Pool, private schema: string, private exchangeManager: ExchangeManager){}
    
    get insertToExchangeSQL() {
        return `
            INSERT INTO ${this.schema}.message_queue (
                queue_id,
                message_id,
                message
            )
            SELECT queue_id, $1, $2
            FROM ${this.schema}.exchange_queue
            WHERE exchange_id = $3
            RETURNING message_id
        `
    }
    private toExchange = async <T>(message: T, exchange: string) => {
        const messageText: string = JSON.stringify(message);
        const exchangeId: string = await this.exchangeManager.getExchangeId(exchange);
        const res = await this.pool.query(
            this.insertToExchangeSQL,
            [uuid(), messageText, exchangeId]
        );

        if (!res.rowCount) {
            console.warn(
                `Exchange ${exchange} has no bound queues, message dropped`, 
                {exchange: {name: exchange, id: exchangeId}}
            );
            return '';
        }

        return res.rows[0].message_id;
    }

    private toQueue = async <T>(message: T, queueName: string) => {
        const messageText = JSON.stringify(message);
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

    produce = <T>(message: T, exchangeName?: string, queueName?: string) => {
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