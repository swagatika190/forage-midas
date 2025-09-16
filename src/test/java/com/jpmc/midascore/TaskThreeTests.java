package com.jpmc.midascore;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import com.jpmc.midascore.foundation.Transaction;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class TaskThreeTests {

    static final Logger logger = LoggerFactory.getLogger(TaskThreeTests.class);

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Test
    void task_three_verifier() throws InterruptedException {

        // Populate users
        userPopulator.populate();

        // Load transactions from file
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            String[] parts = transactionLine.split(", ");
            Transaction transaction = new Transaction(
                Long.parseLong(parts[0].trim()),
                Long.parseLong(parts[1].trim()),
                Double.parseDouble(parts[2].trim()) // changed Float -> Double
            );

            kafkaProducer.sendMessage("orders", transaction);
        }

        // Wait a little for Kafka to process messages
        Thread.sleep(2000);

        logger.info("----------------------------------------------------------");
        logger.info("use your debugger to find out what waldorf's balance is after all transactions are processed");

        // Comment out infinite loop if just testing
        // while (true) {
        //     Thread.sleep(20000);
        //     logger.info("...");
        // }
    }
}
