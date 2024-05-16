package org.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;

public class Parser {
    private final Logger logger = LoggerFactory.getLogger(Parser.class);
    private final LoadBalancer loadBalancer = new LoadBalancer();

    public void processTransactionsFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                processTransactionLine(line);
            }
        } catch (IOException e) {
            logger.error("Ошибка чтения файла: {}", e.getMessage());
        }
    }

    private void processTransactionLine(String line) {
        String[] parts = line.split(",");

        int accountIdSender = Integer.parseInt(parts[0]);
        int accountIdReceiver = Integer.parseInt(parts[1]);
        BigDecimal amount = new BigDecimal(parts[2]);

        loadBalancer.processTransaction(accountIdSender, accountIdReceiver, amount);
    }
}