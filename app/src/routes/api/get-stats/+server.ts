import { json } from '@sveltejs/kit';
import { pool } from '$lib/server/db';

export async function GET({ url }) {
    const defTeamId = Number(url.searchParams.get('defTeamId'));
    const skillPos = url.searchParams.get('skillPos');
    const client = await pool.connect();

    let query: string = 'select * from stats.';
    if (skillPos === "QB") {
        query += `get_def_vs_qb_fantasy_points(${defTeamId});`;
    } else if (skillPos === "RB") {
        query += `get_def_vs_rb_fantasy_points(${defTeamId});`;
    } else {
        client.release();
        return json({ error: "Invalid Skill Position" }, { status: 400 });
    }

    try {
        const result = await client.query(query);
        return json(result.rows);
    }
    catch (error: unknown) {
        if (error instanceof Error) {
            console.error("An error occurred: ", error.message);
        } else {
            console.error("An unknown error has occurre: ", error);
        }
        return json({ error: "Internal Server Error" }, { status: 500 });
    } finally {
        client.release();
        console.log("GET attempt for players stats completed");
    }
}