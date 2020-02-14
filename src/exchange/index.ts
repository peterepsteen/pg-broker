import { Pool } from 'pg';
import uuid = require('uuid');

export class Exchange {
    private constructor(private pool: Pool, private schema: string, name: string) {
        
    }

    private static createSql = (schema: string) => `
        INSERT INTO ${schema}.exchange (exchange_name, exchange_id) 
        VALUES ($1, $2)
        ON CONFLICT(exchange_name) DO UPDATE SET exchange_name=EXCLUDED.exchange_name 
        RETURNING exchange_id
    `;

    //assert creates or validates that the exchange exists, returning the id
    static async assert(pool: Pool, schema: string, name: string) {
        const id = uuid();
        const res = await pool.query(Exchange.createSql(schema), [name, id]);
        const exchangeIdRow = res.rows[0];
        return exchangeIdRow['exchange_id'] as string;
    }
}