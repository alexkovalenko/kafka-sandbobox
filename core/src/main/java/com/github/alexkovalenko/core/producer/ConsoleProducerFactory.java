package com.github.alexkovalenko.core.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Scanner;

import static java.util.Objects.isNull;

public class ConsoleProducerFactory {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleProduser.class);

    public static ConsoleProduser consoleProducer(ConsoleProducerConfig config) {
        return () -> {

        };
    }

    public static ConsoleProduser consoleProducerWithCallback(ConsoleProducerConfig config) {
        return () -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(config.toProperties());
                 Scanner in = new Scanner(System.in)) {
                String line;
                logger.info("Start sending message");
                while (!"exit".equalsIgnoreCase(line = in.nextLine())) {
                    ProducerRecord<String, String> record
                            = new ProducerRecord<>(config.getTopic(), line);
                    producer.send(record, (meta, e) -> {
                        if (isNull(e)) {
                            logger.info("Received new meradata.\n Topic: {}.\n Partition: {}.\n Offset: {}.\n Timestamp: {}.\n",
                                    meta.topic(), meta.partition(), meta.offset(), meta.timestamp());
                        } else {
                            logger.error("Error while sending message to topic {}", config.getTopic());
                        }
                    });
                }
                logger.info("Flushing...");
                producer.flush();
            }
        };
    }

    private static Optional<ProducerRecord<String, String>> getProducerRecord(ConsoleProducerConfig config, String line) {
        if (config.isSplitKey()) {
            if (!line.contains(config.getKeySeparator())) {
                logger.warn("Missing key. Line will be skipped. Msg should be in form key{}value", config.getKeySeparator());
                return Optional.empty();
            }
            String[] msgParts = line.split(config.getKeySeparator());
            String key = msgParts[0];
            String value = msgParts[1];
            return Optional.of(new ProducerRecord<>(config.getTopic(), key, value));
        }
        return Optional.of(new ProducerRecord<>(config.getTopic(), line));
    }
}
