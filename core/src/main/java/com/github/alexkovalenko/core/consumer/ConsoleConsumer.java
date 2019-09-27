package com.github.alexkovalenko.core.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;

public class ConsoleConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleConsumer.class);

    public static void main(String[] args) {
        ConsoleConsumerProperties consoleConsumerProperties = new ConsoleConsumerProperties(args);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consoleConsumerProperties.toProperties());
        consumer.subscribe(Collections.singleton(consoleConsumerProperties.getTopic()));
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(consumer);
        CompletableFuture.runAsync(consumerRunnable);
        logger.info("Type exit to stop.");
        Scanner in = new Scanner(System.in);
        while (!"exit".equalsIgnoreCase(in.nextLine())) ;
        logger.info("Stopping execution...");
        consumerRunnable.shutdown();
    }

    private static class ConsumerRunnable implements Runnable {

        private final KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(KafkaConsumer<String, String> consumer) {
            this.consumer = consumer;
        }

        @Override
        public void run() {
            try {
                while (true) {
                    consumer.poll(Duration.ofMillis(100))
                            .forEach(r ->
                                    logger.info("New msg Key: {}. Value: {}, Partition: {}, Offset: {}",
                                            r.key(), r.value(), r.partition(), r.offset()));
                }
            } catch (Exception e) {
                logger.warn("Received shutdown signal");
                consumer.close();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
