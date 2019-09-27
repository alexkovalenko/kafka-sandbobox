package com.github.alexkovalenko.stream.bankbalance.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class BankTransactionSerde implements Serde<BankTransaction> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Serializer<BankTransaction> serializer = new BankTransactionSerializer();
    private final Deserializer<BankTransaction> deserializer = new BankTransactionDeserializer();

    @Override
    public Serializer<BankTransaction> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<BankTransaction> deserializer() {
        return deserializer;
    }

    public static BankTransactionSerde transactionSerde() {
        return new BankTransactionSerde();
    }

    public static class BankTransactionSerializer implements Serializer<BankTransaction> {

        @Override
        public byte[] serialize(String topic, BankTransaction data) {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public class BankTransactionDeserializer implements Deserializer<BankTransaction> {
        @Override
        public BankTransaction deserialize(String topic, byte[] data) {
            try {
                return OBJECT_MAPPER.readValue(data, BankTransaction.class);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}
