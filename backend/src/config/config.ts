import dotenv from 'dotenv';
import path from 'path';

// Load the correct .env file based on NODE_ENV
const envFile = process.env.NODE_ENV === 'PROD' ? '.env.prod' : '.env.dev';
dotenv.config({ path: path.resolve(__dirname, '../../', envFile) });

export const config = {
  db: {
    user: process.env.DB_USER!,
    password: process.env.DB_PASSWORD!,
    host: process.env.DB_HOST!,
    name: process.env.DB_NAME!,
    port: Number(process.env.DB_PORT!),
  },
  apiSecret: process.env.API_SECRET_KEY!,
};
