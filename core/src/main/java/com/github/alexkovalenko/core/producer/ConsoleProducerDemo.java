package com.github.alexkovalenko.core.producer;

public class ConsoleProducerDemo {
    public static void main(String[] args) {
        ConsoleProducerConfig consoleProducerConfig = new ConsoleProducerConfig(args);
        ConsoleProducerFactory.consoleProducer(consoleProducerConfig).run();
    }
}
