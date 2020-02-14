import { Pool, Client } from 'pg';
import uuid = require('uuid');
import { Message } from '../message';
import { ExchangeManager } from '../exchange';

export class QueueManager {
    constructor(private pool: Pool, private schema: string, private exchangeManager: ExchangeManager) {}

    private static insertQueueSql = (schema: string) => `
        INSERT INTO ${schema}.queue (queue_name, queue_id) 
        VALUES ($1, $2)
        ON CONFLICT(queue_name) DO UPDATE SET queue_name=EXCLUDED.queue_name 
        RETURNING queue_id
    `;

    private static createTriggerStmts = (schema: string, queueName: string, queueId: string) => [`
        CREATE OR REPLACE FUNCTION ${schema}.${queueName}_notify()
        RETURNS trigger AS
        $$
        BEGIN
            PERFORM pg_notify('${queueName}_channel', NEW.queue_id);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql`,

        `DROP TRIGGER IF EXISTS ${queueName}_status ON ${schema}.message_queue RESTRICT`,
        `CREATE TRIGGER ${queueName}_status
            AFTER INSERT
            ON ${schema}.message_queue
            FOR EACH ROW
            WHEN (NEW.queue_id = '${queueId}')
        EXECUTE PROCEDURE ${schema}.${queueName}_notify()`
    ]

    exists = async (queueName: string) => {
        const res = await this.pool.query(
            `SELECT queue_id FROM ${this.schema}.queue WHERE queue_name = $1`, 
            [queueName]
        );
        if (!res.rowCount) {
            return '';
        }
        return res.rows[0].queue_id as string;
    }

    // alias for exists but throws error if not exists
    getQueueId = async (queueName: string): Promise<string> => {
        const id = await this.exists(queueName)
        if (!id) {
            throw new Error(`queue ${queueName} does not exist`);
        }
        return id;
    } 

    get bindSql() {
        return `
            INSERT INTO ${this.schema}.exchange_queue (exchange_id, queue_id)
            VALUES ($1, $2)
            ON CONFLICT(exchange_id, queue_id) DO NOTHING
        `;
    }

    async bindQueueToExchange(queueName: string, exchangeName: string) {
        console.log('DEBUG binding queue to exchange', {queueName, exchangeName});
        const queueID = await this.getQueueId(queueName);
        const exchangeID = await this.exchangeManager.getExchangeId(exchangeName);
        console.log('DEBUG binding queue to exchange', {queueID, exchangeID});
        await this.pool.query(this.bindSql, [exchangeID, queueID]); 
        console.log('DEBUG bound queue to exchange', {queueID, exchangeID});
    }

    //assert creates or validates that the queue exists, returning the id
    async assert(name: string) {
        const id = await this.exists(name);
        if (id) {
            console.log(`DEBUG: ${name} already exists`)
            return id;
        }
        console.log(`DEBUG: creating queue ${name}`)
        return this.createQueue(name);
    }

    private withTransaction = async (fn: () => any) => {
        await this.pool.query('BEGIN');
        try {
            await fn();
        } catch (error) {
            await this.pool.query('ROLLBACK'); 
            console.error(error);  
            throw error;
        }
        await this.pool.query('COMMIT');
    }

    private createQueue = async (name: string) => {
        const id = uuid();

        await this.withTransaction(async () => {
            const sql = QueueManager.insertQueueSql(this.schema);

            const res = await this.pool.query(sql, [name, id]);
            if (!res.rowCount) {
                throw new Error(`No id after inserting queue ${name}`);
            }

            const {queue_id: newQueueId} = res.rows[0];

            const stmts = QueueManager.createTriggerStmts(this.schema, name, newQueueId);

            for (let stmt of stmts) {
                await this.pool.query(stmt);
            }
        })

        return id;
    }
}
