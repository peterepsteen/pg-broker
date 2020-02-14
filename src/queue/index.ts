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
            PERFORM pg_notify('${queueName}_channel', row_to_json(NEW)::text);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql`,

        `DROP TRIGGER IF EXISTS ${queueName}_status ON ${schema}.message_queue RESTRICT`,
        `CREATE TRIGGER ${queueName}_status
            AFTER INSERT OR UPDATE OF message_id
            ON ${schema}.message_queue
            FOR EACH ROW
            WHEN (NEW.queue_id = '${queueId}')
        EXECUTE PROCEDURE ${schema}.${queueName}_notify()`
    ]

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

    subscribe = (queueName: string, callback: (message: Message) => void) => {
        return new Promise(res => this.pool.connect(async (err, client, done) => {
            await client.query(`LISTEN ${queueName}_channel`);
            res();
            client.on('notification', msg => { //TODO: just get message_id and use skip_lock
                if (!msg.payload) {
                    console.warn(`empty message payload: ${msg}`);
                }

                const pgMsg: PostgresMessage = JSON.parse(msg.payload as string);

                const message: Message = {message: JSON.parse(pgMsg.message), name: ''}
                console.log(`DEBUG: sending message`,{message});
                callback(message);
            })
        }))
    }
}

interface PostgresMessage {
    queue_id: string
    message_id: string
    message: string
}