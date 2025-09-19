import pg from 'pg';
import 'dotenv/config';


export const pool = new pg.Pool({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASSWORD || '',
    port: Number(process.env.DB_PORT) || 5432,
    max: 1,
    idleTimeoutMillis: 30000
});