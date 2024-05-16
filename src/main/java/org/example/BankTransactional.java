package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.*;

public class BankTransactional {
    private final Logger logger = LoggerFactory.getLogger(BankTransactional.class);
    private final String URL = "jdbc:postgresql://127.0.0.1:5432/bank";
    private final String USER = "postgres";
    private final String PASSWORD = "postgres";

    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final ConcurrentHashMap<Integer, BlockingQueue<Runnable>> accountTasks = new ConcurrentHashMap<>();

    public void processTransaction(int accountIdSender, int accountIdReceiver, BigDecimal amount) {
        int lowId = Math.min(accountIdSender, accountIdReceiver);
        int highId = Math.max(accountIdSender, accountIdReceiver);

        accountTasks.computeIfAbsent(lowId, k -> new LinkedBlockingQueue<>()).add(() -> {
            accountTasks.computeIfAbsent(highId, k -> new LinkedBlockingQueue<>()).add(() -> {
                try (Connection conn = DriverManager.getConnection(URL, getProperties())) {
                    conn.setAutoCommit(false);
                    updateAccountBalance(conn, accountIdSender, accountIdReceiver, amount);
                    conn.commit();
                } catch (SQLException ex) {
                    logger.error("Ошибка выполнения транзакции для счёта: {}", accountIdSender, ex);
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

    private void updateAccountBalance(Connection conn, int accountIdSender, int accountIdReceiver, BigDecimal amount) throws SQLException {
        // Получение текущего баланса
        BigDecimal currentBalance = getCurrentBalance(conn, accountIdSender);
        if (currentBalance == null) {
            logger.error("Счет не найден: {}", accountIdSender);
            return;
        }

        BigDecimal newBalance = currentBalance.add(amount.negate());
        if (newBalance.compareTo(BigDecimal.ZERO) < 0) {
            logger.error("Недостаточно средств на счете: {}", accountIdSender);
            return;
        }

        PreparedStatement updateStmtSender = conn.prepareStatement("UPDATE accounts SET balance = balance - ? WHERE account_id = ?");
        updateStmtSender.setBigDecimal(1, amount);
        updateStmtSender.setInt(2, accountIdSender);
        if (updateStmtSender.executeUpdate() == 0) {
            conn.rollback();
            throw new SQLException("Ошибка обновления счёта отправителя. Ни одна запись не затронута.");
        }

        // Подготовка запроса для добавления средств на счёт получателя
        PreparedStatement updateStmtReceiver = conn.prepareStatement("UPDATE accounts SET balance = balance + ? WHERE account_id = ?");
        updateStmtReceiver.setBigDecimal(1, amount);
        updateStmtReceiver.setInt(2, accountIdReceiver);
        if (updateStmtReceiver.executeUpdate() == 0) {
            conn.rollback();
            throw new SQLException("Ошибка обновления счёта получателя. Ни одна запись не затронута.");
        }

        logger.info("Успешно выполненная транзакция. Счет отправителя: {}. Счет получателя: {}", accountIdSender, accountIdReceiver);
    }

    private BigDecimal getCurrentBalance(Connection conn, int accountIdSender) throws SQLException {
        PreparedStatement selectStmt = conn.prepareStatement("SELECT balance FROM accounts WHERE account_id = ?");
        selectStmt.setInt(1, accountIdSender);
        ResultSet rs = selectStmt.executeQuery();
        if (rs.next()) {
            return rs.getBigDecimal("balance");
        }
        return null;
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        return props;
    }
}
