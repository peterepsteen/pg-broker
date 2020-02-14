import { Pool, Client } from 'pg';
import uuid = require('uuid');
import { Message } from '../message';

export class QueueManager {
    constructor(private pool: Pool, private schema: string) {}

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

    private static getNextMessage = (schema: string) => `
        DELETE FROM ${schema}.message_queue
        WHERE message_id = (
            SELECT message_id FROM ${schema}.message_queue
            WHERE queue_id = $1
            ORDER BY created_at 
            FOR UPDATE SKIP LOCKED 
            LIMIT 1
        )
        RETURNING message_id, message;
    `

    private exists = async (queueName: string) => {
        const res = await this.pool.query(
            `SELECT queue_id FROM ${this.schema}.queue WHERE queue_name = $1`, 
            [queueName]
        );
        if (!res.rowCount) {
            return '';
        }
        return res.rows[0].queue_id as string;
    }

    //assert creates or validates that the queue exists, returning the id
    async assert(name: string) {
        const id = await this.exists(name);
        if (id) {
            console.log(`DEBUG: ${name} already exists`)
            return id;
        }
        return this.createQueue(name);
    }

    private createQueue = async (name: string) => {
        const newId = uuid();
        await this.pool.query('BEGIN');
        let newQueueId;
        try {
            newQueueId = await this.createQueueSafe(name, newId)
        } catch (error) {
            await this.pool.query('ROLLBACK');    
            throw error;
        }
        await this.pool.query('COMMIT');
        return newQueueId;
    }

    private createQueueSafe = async (name: string, id: string) => {
        const res = await this.pool.query(QueueManager.insertQueueSql(this.schema), [name, id]);
        if (!res.rowCount) {
            throw new Error(`No id after inserting queue ${name}`);
        }

        const queueIdRow = res.rows[0];
        const newQueueId = queueIdRow['queue_id'] as string;

        const stmts = QueueManager.createTriggerStmts(this.schema, name, newQueueId);

        for (let stmt of stmts) {
            await this.pool.query(stmt);
        }
        return newQueueId;
    }

    private tryGetMessage = async <T>(queueId: string, callback: (message: T) => void): Promise<Boolean> => {
        console.log('DEBUG trying to get next message', {queueId});
        const res = await this.pool.query(QueueManager.getNextMessage(this.schema), [queueId]);
        if (!res.rowCount) {
            console.log('DEBUG no message available', {queueId});
            return false;
        }
        const message = res.rows[0];
        callback(JSON.parse(message.message) as T);
        return true;
    }

    // TODO: seperate this to Consumer
    subscribe = async <T>(queueName: string, callback: (message: T) => void) => {
        const subscriberId = uuid();

        const id = await this.exists(queueName);
        if (!id) {
            throw new Error(`queue ${queueName} does not exist`);
        }

        //initially, see if anything is currently queued
        while (await this.tryGetMessage(id, callback)){}

        return new Promise(res => this.pool.connect(async (err, client, done) => {
            await client.query(`LISTEN ${queueName}_channel`);
            res();
            client.on('notification', msg => {
                const queueId = msg.payload as string;
                console.log('DEBUG received notification', {queueId})
                this.tryGetMessage(queueId, callback);
            })
        }))
    }
}
