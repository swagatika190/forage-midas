package com.jpmc.midascore;

import com.jpmc.midascore.component.DatabaseConduit;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);

    private final DatabaseConduit databaseConduit;

    public KafkaConsumer(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "group1", containerFactory = "kafkaListenerContainerFactory")
    public void consume(Transaction transaction) {
        logger.info("Consumed transaction: {}", transaction);

        try {
            // Deduct from sender
            UserRecord sender = databaseConduit.findById(transaction.getSenderId());
            sender.setBalance(sender.getBalance() - (float) transaction.getAmount());
            databaseConduit.save(sender);

            // Add to recipient
            UserRecord recipient = databaseConduit.findById(transaction.getRecipientId());
            recipient.setBalance(recipient.getBalance() + (float) transaction.getAmount());
            databaseConduit.save(recipient);

        } catch (Exception e) {
            logger.error("Error updating balances: {}", e.getMessage());
        }

        // Log Waldorf's balance if this is him
        try {
            UserRecord waldorf = databaseConduit.findByName("Waldorf");
            logger.info("Waldorf's current balance: {}", waldorf.getBalance());
        } catch (Exception ignored) {}
    }
}
