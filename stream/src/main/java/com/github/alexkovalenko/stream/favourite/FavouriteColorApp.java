package com.github.alexkovalenko.stream.favourite;

import com.github.alexkovalenko.stream.StreamRunner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.nonNull;

public class FavouriteColorApp {
    public static void main(String[] args) {

        List<String> supportedColors = Arrays.asList("green", "blue", "red");

        Serde<String> stringSerde = Serdes.String();
        Serde<Long> longSerde = Serdes.Long();

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-color-app");
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, stringSerde.getClass().getName());
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> favouriteColorInput = builder.stream("favourite-color-input");
        favouriteColorInput
                .filter((key, value) -> nonNull(value) && value.contains(","))
                .map((key, value) -> {
                    String[] valueParts = value.split(",");
                    return new KeyValue<>(valueParts[0], valueParts[1]);
                })
                .filter((name, color) -> supportedColors.contains(color))
                .to("favourite-color-tmp");

        KTable<String, String> favouriteColorsTable = builder.table("favourite-color-tmp");
        KTable<String, Long> colorCounts = favouriteColorsTable
                .groupBy((user, colour) -> new KeyValue<>(colour, colour))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("ColorCounts")
                        .withKeySerde(stringSerde).withValueSerde(longSerde));
        colorCounts.toStream().to("favourite-color-output", Produced.with(stringSerde, longSerde));

        StreamRunner.runWithShutdownHook(config, builder);
    }
}

