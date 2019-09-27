package com.github.alexkovalenko.stream.bankbalance;

import com.github.alexkovalenko.stream.bankbalance.dto.BankTransaction;
import com.github.alexkovalenko.stream.bankbalance.dto.BankTransactionSerde;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BankTransactionProducer {

    private static final List<String> NAMES = List.of("Alex", "Bob", "Peter", "Sandra");
    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        Properties config = new Properties();
        config.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BankBalanceAppConfig.BOOTSTRAP_SERVER);
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, BankTransactionSerde.BankTransactionSerializer.class.getName());
        config.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        config.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        config.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        config.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        KafkaProducer<String, BankTransaction> producer = new KafkaProducer<>(config);
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1);
        executorService.scheduleWithFixedDelay(() -> sendRandomTransactions(producer, 100), 0, 1, TimeUnit.SECONDS);
        addShutdownHook(producer, executorService);
    }

    private static void sendRandomTransactions(KafkaProducer<String, BankTransaction> producer, long numOfMessages) {
        for (int i = 0; i < numOfMessages; i++) {
            BankTransaction transaction = randomTransaction();
            producer.send(new ProducerRecord<>(BankBalanceAppConfig.BANK_TRANSACTION_TOPIC_NAME, transaction.getName(), transaction));
        }
    }

    private static BankTransaction randomTransaction() {
        return new BankTransaction(NAMES.get(RANDOM.nextInt(3)), RANDOM.nextInt(1000), Instant.now().toString());
    }


    private static void addShutdownHook(KafkaProducer<String, BankTransaction> producer, ScheduledExecutorService executorService) {
        Runtime.getRuntime().addShutdownHook(new Thread("BankTransactionProducer-ShutdownHook") {
            @Override
            public void run() {
                executorService.shutdown();
                producer.flush();
                producer.close();
            }
        });
    }
}
