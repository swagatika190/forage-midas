package com.jpmc.midascore;

import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.foundation.Transaction;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFiveTests {
    static final Logger logger = LoggerFactory.getLogger(TaskFiveTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private BalanceQuerier balanceQuerier;

    @Test
    void task_five_verifier() throws InterruptedException {
        // Step 1: Populate users
        userPopulator.populate();

        // Step 2: Load transactions from file
        String[] transactionLines = fileLoader.loadStrings("/test_data/rueiwoqp.tyruei");

        // Step 3: Convert each line to a Transaction object and send to Kafka
        for (String transactionLine : transactionLines) {
            String[] parts = transactionLine.split(", ");
            Transaction transaction = new Transaction(
                Long.parseLong(parts[0].trim()),   // senderId
                Long.parseLong(parts[1].trim()),   // recipientId
                Float.parseFloat(parts[2].trim())  // amount
            );
            kafkaProducer.sendMessage("orders", transaction);
        }

        // Step 4: Wait for transactions to be consumed
        Thread.sleep(2000);

        // Step 5: Log balances for submission
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("----------------------------------------------------------");
        logger.info("submit the following output to complete the task (include begin and end output denotations)");

        StringBuilder output = new StringBuilder("\n").append("---begin output ---").append("\n");

        // Assuming user IDs from 0 to 12
        for (int i = 0; i <= 12; i++) {
            try {
                float amount = balanceQuerier.getBalance(i);   // get float balance
                Balance balance = new Balance(amount);         // wrap in Balance object
                output.append(balance.toString()).append("\n");
            } catch (RuntimeException e) {
                output.append("User " + i + " not found\n");
            }
        }

        output.append("---end output ---");
        logger.info(output.toString());
    }
}
