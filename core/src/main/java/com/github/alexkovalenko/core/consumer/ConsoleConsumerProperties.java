package com.github.alexkovalenko.core.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

import static java.util.Objects.nonNull;


public class ConsoleConsumerProperties {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String DEFAULT_TOPIC = "adam";
    private static final String DEFAULT_GROUP = "first_mans";
    private final String bootstrapServers;
    private final String topic;
    private final String group;

    public ConsoleConsumerProperties(String[] args) {
        this(false, args);
    }

    public ConsoleConsumerProperties(boolean noGroup, String[] args) {
        if (nonNull(args) && args.length == 3) {
            this.bootstrapServers = args[0];
            this.topic = args[1];
            this.group = noGroup ? null : args[2];
        } else {
            this.bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
            this.topic = DEFAULT_TOPIC;
            this.group = noGroup ? null : DEFAULT_GROUP;
        }
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    public Properties toProperties() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.getBootstrapServers());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        if (nonNull(this.group)) {
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.group);
        }
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");//todo: add initialization for tihs property
        return properties;
    }
}
