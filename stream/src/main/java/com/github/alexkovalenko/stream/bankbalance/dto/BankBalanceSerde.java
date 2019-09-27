package com.github.alexkovalenko.stream.bankbalance.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

public class BankBalanceSerde implements Serde<BankBalance> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final Serializer<BankBalance> serializer = new BankBalanceSerializer();
    private final Deserializer<BankBalance> deserializer = new BankBalanceDeserializer();

    @Override
    public Serializer<BankBalance> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<BankBalance> deserializer() {
        return deserializer;
    }

    public static BankBalanceSerde balanceSerde() {
        return new BankBalanceSerde();
    }

    public static class BankBalanceSerializer implements Serializer<BankBalance> {

        @Override
        public byte[] serialize(String topic, BankBalance data) {
            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                throw new IllegalStateException(e);
            }
        }
    }

    public class BankBalanceDeserializer implements Deserializer<BankBalance> {
        @Override
        public BankBalance deserialize(String topic, byte[] data) {
            try {
                return OBJECT_MAPPER.readValue(data, BankBalance.class);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
    }

}
