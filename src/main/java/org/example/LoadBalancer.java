package org.example;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.*;

public class LoadBalancer {
    private final TransactionProcessor transactionProcessor = new TransactionProcessor();

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<Integer, BlockingQueue<Runnable>> accountTasks = new ConcurrentHashMap<>();

    public void processTransaction(int accountIdSender, int accountIdReceiver, BigDecimal amount) {
        int lowId = Math.min(accountIdSender, accountIdReceiver);
        int highId = Math.max(accountIdSender, accountIdReceiver);

        accountTasks.computeIfAbsent(lowId, k -> new LinkedBlockingQueue<>()).add(() -> {
            accountTasks.computeIfAbsent(highId, k -> new LinkedBlockingQueue<>()).add(() -> {
                try {
                    transactionProcessor.updateAccountBalance(accountIdSender, accountIdReceiver, amount);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });

            startQueueProcessor(lowId);
            startQueueProcessor(highId);
        });

        startQueueProcessor(lowId);
    }

    private void startQueueProcessor(int accountId) {
        BlockingQueue<Runnable> queue = accountTasks.get(accountId);
        if (queue != null && queue.size() == 1) {
            executorService.submit(() -> {
                Runnable task;
                while ((task = queue.poll()) != null) {
                    task.run();
                }
            });
        }
    }
}
