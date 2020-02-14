import { Pool } from 'pg';
import uuid = require('uuid');

export class ExchangeManager {
    constructor(private pool: Pool, private schema: string) {}

    private static createSql = (schema: string) => `
        INSERT INTO ${schema}.exchange (exchange_name, exchange_id) 
        VALUES ($1, $2)
        ON CONFLICT(exchange_name) DO UPDATE SET exchange_name=EXCLUDED.exchange_name 
        RETURNING exchange_id
    `;

    //assert creates or validates that the exchange exists, returning the id
    async assert(name: string) {
        const id = uuid();
        const res = await this.pool.query(ExchangeManager.createSql(this.schema), [name, id]);
        const exchangeIdRow = res.rows[0];
        return exchangeIdRow['exchange_id'] as string;
    }


    private get getExchangeIdSQL() {
        return  `
            SELECT exchange_id
            FROM ${this.schema}.exchange
            WHERE exchange_name = $1
        `
    };

    async getExchangeId(name: string) {
        const response = await this.pool.query(this.getExchangeIdSQL, [name]);
        if (!response.rowCount) {
            throw new Error(`exchange ${name} not found`);
        }

        const {exchange_id} = response.rows[0];
        return exchange_id;
    }
}