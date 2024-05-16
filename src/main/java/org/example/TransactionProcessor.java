package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Properties;

public class TransactionProcessor {
    private final Logger logger = LoggerFactory.getLogger(TransactionProcessor.class);

    private final String URL = "jdbc:postgresql://127.0.0.1:5432/bank";
    private final String USER = "postgres";
    private final String PASSWORD = "postgres";

    public void updateAccountBalance(int accountIdSender, int accountIdReceiver, BigDecimal amount) throws SQLException {
        try (Connection conn = DriverManager.getConnection(URL, getProperties())) {
            conn.setAutoCommit(false);
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

            conn.commit();
            logger.info("Успешно выполненная транзакция. Счет отправителя: {}. Счет получателя: {}", accountIdSender, accountIdReceiver);
        } catch (SQLException ex) {
            logger.error("Ошибка выполнения транзакции для счёта: {}", accountIdSender, ex);
        }
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
