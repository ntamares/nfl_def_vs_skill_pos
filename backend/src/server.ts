import express from 'express';

const app = express();
const PORT = process.env.PORT || 5000;
const cors = require('cors');
import routes from './routes';

app.use(cors());
app.use(express.json());
app.use('/api', routes);

// Start the server
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});