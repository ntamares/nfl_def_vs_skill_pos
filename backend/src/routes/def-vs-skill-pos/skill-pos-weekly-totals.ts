import express from 'express';
import pool from '../../db/index';

const router = express.Router();

router.post('/', async (req, res) => {
    const {teamId, position, startWeek, endWeek } = req.body;
    try {
        const result = await pool.query(
            'SELECT * FROM stats.get_def_vs_position_week_totals($1, $2, $3, $4)',
            [teamId, position, startWeek, endWeek]
        );
        res.json(result.rows);
    }
    catch (err: unknown) {
        if (err instanceof Error){
            res.status(500).json({
                err: 'Database error',
                details: err.message
            });
        }
        else {
            res.status(500).json({ error: 'Unknown error' });
        }
    }  
});

router.use((req, res) => {
    res.status(404).json({ error: 'Route not found in skill-pos-weekly-totals' });
});

export default router;