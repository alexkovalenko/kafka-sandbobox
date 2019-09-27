package com.github.alexkovalenko.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class StreamRunner {

    public static void runWithShutdownHook(Properties config, StreamsBuilder builder) {
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        final CountDownLatch
                latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.cleanUp();
            streams.start();

            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
