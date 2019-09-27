package com.github.alexkovalenko.core.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class ConsoleConsumerAssignSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleConsumerAssignSeek.class);

    public static void main(String[] args) {
        ConsoleConsumerProperties consoleConsumerProperties = new ConsoleConsumerProperties(true, args);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consoleConsumerProperties.toProperties());) {
            TopicPartition partition = new TopicPartition(consoleConsumerProperties.getTopic(), 0);
            long offsetsToReadFrom = 15;
            consumer.assign(Collections.singleton(partition));

            consumer.seek(partition, offsetsToReadFrom);

            int numberOfMessagesToConsum = 10;
            int numberOfMessagesConsumed = 0;
            boolean keepPolling = true;
            while (keepPolling) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> r : records) {
                    numberOfMessagesConsumed++;
                    logger.info("New msg Key: {}. Value: {}, Partition: {}, Offset: {}",
                            r.key(), r.value(), r.partition(), r.offset());
                    if (numberOfMessagesConsumed == numberOfMessagesToConsum) {
                        keepPolling = false;
                        break;
                    }
                }

            }
            logger.info("Stopping execution...");
        }
    }
}
