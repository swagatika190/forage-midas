package com.jpmc.midascore;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaProducer implements CommandLineRunner {

    @Autowired
    private KafkaTemplate<String, Transaction> kafkaTemplate;

    private final String TOPIC = "orders";

    @Override
    public void run(String... args) throws Exception {
        System.out.println("Sending 4 transactions...");

        for (int i = 1; i <= 4; i++) {
            Transaction tx = new Transaction(1, 2, 100.5 * i);
            kafkaTemplate.send(TOPIC, tx);
            System.out.println("Produced transaction: " + tx);
        }

        System.out.println("All 4 transactions sent!");
    }
    public void sendMessage(String topic, Transaction transaction) {
        kafkaTemplate.send(topic, transaction);
    }
}
