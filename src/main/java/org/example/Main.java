package org.example;

public class Main {
    public static void main(String[] args) {
        Parser parser = new Parser();
        parser.processTransactionsFromFile("src/main/resources/input_file.txt");
    }
}