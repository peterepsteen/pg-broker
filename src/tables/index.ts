const getSqlStatments = (schema: string) => ({
    createSchema: `CREATE SCHEMA IF NOT EXISTS ${schema}`,

    exchangeTable: `
        CREATE TABLE IF NOT EXISTS ${schema}.exchange (
            exchange_name varchar(500) NOT NULL PRIMARY KEY,
            exchange_id varchar(36) UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `,

    queueTable: `
        CREATE TABLE IF NOT EXISTS ${schema}.queue (
            queue_name varchar(500) NOT NULL PRIMARY KEY,
            queue_id varchar(36) UNIQUE NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `,

    exchangeQueueTable: `
        CREATE TABLE IF NOT EXISTS ${schema}.exchange_queue (
            exchange_id varchar(36) REFERENCES ${schema}.exchange(exchange_id) ON DELETE CASCADE,
            queue_id varchar(36) REFERENCES ${schema}.queue(queue_id) ON DELETE CASCADE,
            CONSTRAINT exchange_queue_pk PRIMARY KEY(exchange_id, queue_id)
        )
    `,

    messageQueueTable: `
        CREATE TABLE IF NOT EXISTS ${schema}.message_queue (
            queue_id varchar(36) REFERENCES ${schema}.queue(queue_id) ON DELETE CASCADE,
            message_id varchar(36) NOT NULL,
            message text,
            CONSTRAINT message_queue_pk PRIMARY KEY(message_id, queue_id)
        )
    `,
});

export const getTableDefinitions = (schema: string) => {
    const sqlStmts = getSqlStatments(schema)
    return [sqlStmts.createSchema, sqlStmts.exchangeTable, sqlStmts.queueTable, sqlStmts.exchangeQueueTable, sqlStmts.messageQueueTable];
};