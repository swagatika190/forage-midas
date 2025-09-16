package com.jpmc.midascore;

import org.springframework.boot.CommandLineRunner;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.jpmc.midascore.foundation.Transaction;

@SpringBootApplication
public class MidasCoreApplication {

    public static void main(String[] args) {
        SpringApplication.run(MidasCoreApplication.class, args);
    }
//    @Autowired
//    private KafkaProducer kafkaProducer;
//    
//    @Bean
//    public CommandLineRunner sendTestMessage() {
//        return args -> {
//        	Transaction transaction = new Transaction(1, 2, 100.5f);
//        	// Send it to the "orders" topic
//            kafkaProducer.sendMessage("orders", transaction);
//
//            System.out.println("Test transaction sent!");
//        };
//    }

}
