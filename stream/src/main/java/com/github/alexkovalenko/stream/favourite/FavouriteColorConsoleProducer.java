package com.github.alexkovalenko.stream.favourite;

import com.github.alexkovalenko.core.producer.ConsoleProducerConfig;
import com.github.alexkovalenko.core.producer.ConsoleProducerFactory;

public class FavouriteColorConsoleProducer {
    public static void main(String[] args) {
        ConsoleProducerConfig config = new ConsoleProducerConfig(args);
        ConsoleProducerFactory.consoleProducer(config).run();
    }
}
