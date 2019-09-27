package com.github.alexkovalenko.stream.bankbalance;

import com.github.alexkovalenko.stream.StreamRunner;
import com.github.alexkovalenko.stream.bankbalance.dto.BankBalance;
import com.github.alexkovalenko.stream.bankbalance.dto.BankTransaction;
import com.github.alexkovalenko.stream.bankbalance.dto.BankTransactionSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;

import static com.github.alexkovalenko.stream.bankbalance.dto.BankBalanceSerde.balanceSerde;
import static com.github.alexkovalenko.stream.bankbalance.dto.BankTransactionSerde.transactionSerde;

public class BankBalanceApp {
    private static final Logger LOGGER = LoggerFactory.getLogger(BankBalanceApp.class);

    public static void main(String[] args) {

        Properties config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, BankBalanceAppConfig.APPLICATION_ID);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BankBalanceAppConfig.BOOTSTRAP_SERVER);
        config.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, BankTransactionSerde.class.getName());
        config.setProperty(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.setProperty(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, BankTransaction> transactionsStream = builder.stream(BankBalanceAppConfig.BANK_TRANSACTION_TOPIC_NAME);
        BankBalance initialBalance = new BankBalance();
        initialBalance.setAmount(0);
        initialBalance.setTime(Instant.ofEpochMilli(0).toString());
        transactionsStream
                .groupByKey(Grouped.with(Serdes.String(), transactionSerde()))
                .aggregate(() -> initialBalance, (key, value, aggregate) -> aggregateTransaction(value, aggregate),
                        Materialized.<String, BankBalance, KeyValueStore<Bytes, byte[]>>as("bank-balance-agg")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(balanceSerde()))
                .toStream().to(BankBalanceAppConfig.BANK_BALANCE_TOPIC_NAME, Produced.with(Serdes.String(), balanceSerde()));
        StreamRunner.runWithShutdownHook(config, builder);
    }

    private static BankBalance aggregateTransaction(BankTransaction transaction, BankBalance balance) {
        long transactionMillis = Instant.parse(transaction.getTime()).toEpochMilli();
        long balanceMillis = Instant.parse(balance.getTime()).toEpochMilli();
        String balanceTime = Instant.ofEpochMilli(Math.max(transactionMillis, balanceMillis)).toString();
        return new BankBalance(balance.getTransactionCounter() + 1, transaction.getName(), balance.getAmount() + transaction.getAmount(), balanceTime);
    }
}
