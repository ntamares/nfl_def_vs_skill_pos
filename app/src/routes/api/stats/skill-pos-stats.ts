import pool from '$lib/server/db';
import { json } from '@sveltejs/kit';

export async function GET () {
    const client = await pool.connect();
    
    try {
        let query: string = 'select * from stats.get_def_vs_qb_fantasy_points';
        let params: [] = [];
        const result = await client.query(query, params);
        
        return json(result.rows);
    }
    catch (error: unknown) {
        if (error instanceof Error) {
            console.error("An error occurred: ", error.message);
        } else {
            console.error("An unknown error has occurre: ", error);
        } 
    } finally {
        client.release();
        console.log("GET attempt for players stats completed");
    }
}