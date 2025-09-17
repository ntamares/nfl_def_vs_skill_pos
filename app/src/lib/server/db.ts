import pg from 'pg';

const pool = new pg.Pool({
    user: 'user',
    host: 'localhost',
    database: 'db',
    password: 'pw',
    port: 5432,
    max: 1,
    idleTimeoutMillis: 30000
});

export default pool;