package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.BalanceQuerier;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskFourTests {

    static final Logger logger = LoggerFactory.getLogger(TaskFourTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private BalanceQuerier balanceQuerier;

    // Replace this with Wilbur's actual ID from your test data
    private static final long WILBUR_ID = 2L;

    @Test
    void task_four_verifier() throws InterruptedException {
        // Populate users
        userPopulator.populate();

        // Load transactions from file
        String[] transactionLines = fileLoader.loadStrings("/test_data/alskdjfh.fhdjsk");
        for (String transactionLine : transactionLines) {
            String[] parts = transactionLine.split(", ");
            Transaction transaction = new Transaction(
                Long.parseLong(parts[0].trim()),
                Long.parseLong(parts[1].trim()),
                Float.parseFloat(parts[2].trim())
            );

            kafkaProducer.sendMessage("orders", transaction);
        }

        // Wait a little for Kafka to process messages
        Thread.sleep(2000);

        // Get Wilbur's balance
        float wilburBalance = balanceQuerier.getBalance(WILBUR_ID);
        logger.info("Wilbur's final balance: {}", wilburBalance);

        // Optional: remove infinite loop for automated testing
        // while (true) {
        //     Thread.sleep(20000);
        //     logger.info("...");
        // }
    }
}
