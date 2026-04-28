import express from "express";
import accountRoutes from './src/routes/account.routes.js';
import transactionRoutes from './src/routes/transaction.routes.js';
import './src/services/outbox.worker.js';
import './src/services/orchestrator.service.js';
import './src/consumers/audit.consumer.js';
import './src/services/fraud.worker.js';
import './src/consumers/notification.consumer.js';
import './src/consumers/ledger.consumer.js';
import './src/consumers/analytics.consumer.js';

const app = express();

// Middleware
app.use(express.json());

// Main entry
app.get("/", (req, res) => {
    res.send("Banking App Native API is running!");
});

// Routes integration
app.use('/api/accounts', accountRoutes);
app.use('/api/transactions', transactionRoutes);

// Global Error Handler
app.use((err, req, res, next) => {
    console.error(`[Error Handler]: ${err.message}`);
    res.status(err.status || 500).json({
        success: false,
        message: err.message || 'Internal Server Error'
    });
});

app.listen(3000, () => {
    console.log("Server is running on port 3000");
});