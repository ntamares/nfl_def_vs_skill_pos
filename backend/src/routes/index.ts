import { Router } from 'express';
import skillPosWeeklyTotalsRouter from './def-vs-skill-pos/skill-pos-weekly-totals';

const router = Router();
console.log("Root route reached");

router.get('/', (req, res) => {
  console.log('Hit GET in root');
  res.json({
    message: 'Welcome to the NFL Defense vs Skill Position API',
    version: '1.0.0',
    status: 'online'
  });
});

router.get('/weekly-total/test', (req, res) => {
  res.json({ message: 'Direct test route working!' });
});

router.use('/weekly-total', skillPosWeeklyTotalsRouter);

router.use((req, res) => {
  res.status(404).json({ error: 'Route not found in main router' });
});

export default router;