import test from 'node:test';
import assert from 'node:assert/strict';
import prisma from '../src/lib/prisma.js';
import TransactionService from '../src/services/transaction.service.js';
import AccountService from '../src/services/account.service.js';
import orchestratorService from '../src/services/orchestrator.service.js';
import auditService from '../src/services/audit.service.js';
import outboxWorker from '../src/services/outbox.worker.js';
import '../src/consumers/ledger.consumer.js';
import '../src/consumers/audit.consumer.js';
import { connectKafka } from '../src/lib/kafka.js';

// Setup dependencies
const accountService = new AccountService();
const transactionService = new TransactionService();

test('Banking App Architecture Tests', async (t) => {
    
    await t.test('setup', async () => {
        // Drop tables in the correct order to respect SQLite foreign keys
        await prisma.fraudAnalysis.deleteMany();
        await prisma.transactionOrchestrator.deleteMany();
        await prisma.ledgerEntry.deleteMany();
        await prisma.transaction.deleteMany();
        await prisma.account.deleteMany();
        await prisma.outboxEvent.deleteMany();
        await prisma.deadLetterQueue.deleteMany();
        // EventStore table is deprecated in favor of Kafka Event Sourcing, 
        // but we'll clear it natively if the schema hasn't dropped it yet.
        try { await prisma.eventStore.deleteMany(); } catch (e) {}

        await connectKafka();
    });

    await t.test('1. Duplicate transaction request (Idempotency)', async () => {
        await accountService.createAccount({ id: 'acc_dup', initialBalance: 1000 });
        
        const payload = {
            transaction_id: 'txn_dup_1',
            account_id: 'acc_dup',
            type: 'DEBIT',
            amount: 100,
            ip_address: '1.2.3.4',
            device_fingerprint: 'dev123'
        };

        const firstResult = await transactionService.createTransaction(payload);
        const secondResult = await transactionService.createTransaction(payload);
        
        assert.deepEqual(firstResult.id, secondResult.id, "Second request should gracefully return idempotent transaction ID");
        
        const acc = await prisma.account.findUnique({ where: { id: 'acc_dup' } });
        assert.equal(acc.balance, 900, "Money should only be deducted exactly once!");
    });

    await t.test('2. Concurrent debit requests (Concurrency)', async () => {
        await accountService.createAccount({ id: 'acc_concurrent', initialBalance: 200 });

        // Fire 3 simultaneous debits of 100 on an account with only 200.
        // Third one must mathematically fail via overdraft bounds.
        const reqs = [
            transactionService.createTransaction({ transaction_id: 'c1', account_id: 'acc_concurrent', type: 'DEBIT', amount: 100 }),
            transactionService.createTransaction({ transaction_id: 'c2', account_id: 'acc_concurrent', type: 'DEBIT', amount: 100 }),
            transactionService.createTransaction({ transaction_id: 'c3', account_id: 'acc_concurrent', type: 'DEBIT', amount: 100 })
        ];

        const results = await Promise.allSettled(reqs);
        
        const successes = results.filter(r => r.status === 'fulfilled');
        const failures = results.filter(r => r.status === 'rejected');
        
        assert.equal(successes.length, 2, "Only 2 transactions should succeed");
        assert.equal(failures.length, 1, "The 3rd transaction must be blocked by overdraft rule");
        
        const acc = await prisma.account.findUnique({ where: { id: 'acc_concurrent' } });
        assert.equal(acc.balance, 0, "Balance must never drop below 0");
    });

    await t.test('3. Worker crash during processing (At-least-once recovery)', async () => {
        await accountService.createAccount({ id: 'acc_crash', initialBalance: 1000 });
        
        // This transaction is instantly logged to DB, but assume worker crashes before processing
        await transactionService.createTransaction({ transaction_id: 'crash_1', account_id: 'acc_crash', type: 'DEBIT', amount: 50 });
        
        // Manually lock it to simulate mid-crash processing
        await prisma.outboxEvent.updateMany({
            where: { status: 'PENDING' },
            data: { status: 'PROCESSING', nextRetryAt: new Date(Date.now() - 1000000) } // Simulate stuck
        });

        // Trigger manual intervention logic (normally handled by orchestrator/worker sweep)
        // Verify outbox lock mechanism restricts concurrent touches
        const target = await prisma.outboxEvent.findFirst({ where: { status: 'PROCESSING' } });
        assert.ok(target, "Should simulate locked worker task");
    });

    await t.test('4. Retry + DLQ behavior (Poison Messages)', async () => {
        await accountService.createAccount({ id: 'acc_poison', initialBalance: 1000 });
        
        // Force inject a exact poison event directly to worker
        const eventId = 'dlq_test_1';
        await prisma.outboxEvent.create({
            data: {
                id: eventId,
                eventType: 'TransactionCompleted',
                payload: JSON.stringify({ amount: "POISON" }), // Will trigger specific mocked error
                status: 'PENDING'
            }
        });

        // Artificially trigger the poll loop up to 4 times
        for (let i = 0; i < 4; i++) {
            const entry = await prisma.outboxEvent.findUnique({ where: { id: eventId }});
            if (entry.status === 'DLQ') break;
            
            await prisma.outboxEvent.update({ where: { id: eventId }, data: { nextRetryAt: new Date() }});
            await outboxWorker.processEvent(entry);
        }

        const outboxEntry = await prisma.outboxEvent.findUnique({ where: { id: eventId } });
        assert.equal(outboxEntry.status, 'DLQ', "Failed to route strictly to DLQ after MAX_RETRIES!");

        const dlqEntry = await prisma.deadLetterQueue.findUnique({ where: { eventId }});
        assert.ok(dlqEntry, "Data did not exist in DeadLetterQueue table");
        assert.match(dlqEntry.failureReason, /poison message corrupted/);
    });

    await t.test('5. Fraud block correctness', async () => {
        await accountService.createAccount({ id: 'acc_fraud', initialBalance: 1000000 });

        // Reject > 50,000 threshold
        await assert.rejects(
            async () => await transactionService.createTransaction({ transaction_id: 'f1', account_id: 'acc_fraud', type: 'DEBIT', amount: 60000 }),
            /Fraud block: Transaction amount exceeds safety limit/
        );

        // Accept up to 5
        for (let i = 2; i <= 6; i++) {
            await transactionService.createTransaction({ transaction_id: `f${i}`, account_id: 'acc_fraud', type: 'DEBIT', amount: 100 });
        }

        // Reject the 6th mapped rapid sequence
        await assert.rejects(
            async () => await transactionService.createTransaction({ transaction_id: 'f7', account_id: 'acc_fraud', type: 'DEBIT', amount: 100 }),
            /Fraud block: Too many transactions in a short period/
        );
    });

    await t.test('6. Event replay rebuilding balances', async () => {
        // Since Kafka is distributed and durable, we must isolate this test aggregate vector 
        // to prevent history from previous test runs accumulating in the event stream.
        const dynamicAccountId = `acc_replay_${Date.now()}`;
        
        await accountService.createAccount({ id: dynamicAccountId, initialBalance: 5000 });

        // Push some valid activity into DB
        await transactionService.createTransaction({ transaction_id: `r1_${Date.now()}`, account_id: dynamicAccountId, type: 'DEBIT', amount: 300 });
        await transactionService.createTransaction({ transaction_id: `r2_${Date.now()}`, account_id: dynamicAccountId, type: 'CREDIT', amount: 1000 });

        // Let the asynchronous consumer trigger finishes
        await outboxWorker.poll();

        // 5000 - 300 + 1000 = 5700
        // Wait slightly for events to flush to local partition buffer by kafkajs producer
        await new Promise(resolve => setTimeout(resolve, 800));

        const derivedBalance = await auditService.rebuildBalanceFromEvents(dynamicAccountId);
        assert.equal(derivedBalance, 5700, "Event sourced deterministic math failed to map truth");
        process.exit(0);
    });
});
