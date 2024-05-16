package org.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;

public class Parser {
    private final BankTransactional bankTransactional = new BankTransactional();

    public void processTransactionsFromFile(String filePath) {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                processTransactionLine(line);
            }
        } catch (IOException e) {
            System.err.println("Ошибка чтения файла: " + e.getMessage());
        }
    }

    private void processTransactionLine(String line) {
        String[] parts = line.split(",");

        int accountIdSender = Integer.parseInt(parts[0]);
        int accountIdReceiver = Integer.parseInt(parts[1]);
        BigDecimal amount = new BigDecimal(parts[2]);

        bankTransactional.processTransaction(accountIdSender, accountIdReceiver, amount);
    }
}