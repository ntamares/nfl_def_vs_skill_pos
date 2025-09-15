import { config } from '../config/config';

const Pool = require('pg').Pool;
const pool = new Pool({
    user: config.db.user,
    host: config.db.host,
    database: config.db.name,
    password: config.db.password,
    port: config.db.port,
});

export default pool;