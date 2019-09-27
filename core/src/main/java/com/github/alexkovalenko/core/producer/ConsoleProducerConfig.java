package com.github.alexkovalenko.core.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static java.util.Objects.nonNull;

public class ConsoleProducerConfig {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String DEFAULT_TOPIC = "adam";
    private static final String DEFAULT_KEY_SEPARATOR = ",";
    private final String bootstrapServers;
    private final String topic;
    private boolean splitKey;

    public ConsoleProducerConfig(String ... args) {
        if (nonNull(args) && args.length >= 2) {
            this.bootstrapServers = args[0];
            this.topic = args[1];
            this.splitKey = args.length > 2 && "true".equalsIgnoreCase(args[2]);
        } else {
            this.bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
            this.topic = DEFAULT_TOPIC;
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public boolean isSplitKey() {
        return splitKey;
    }

    public String getKeySeparator() {
        return DEFAULT_KEY_SEPARATOR;
    }



    public Properties toProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
