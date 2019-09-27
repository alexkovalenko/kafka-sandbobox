package com.github.alexkovalenko.stream.bankbalance.dto;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class BankTransaction {
    private final String name;
    private final long amount;
    private final String time;

    @JsonCreator
    public BankTransaction(@JsonProperty("name") String name,
                           @JsonProperty("amount") long amount,
                           @JsonProperty("time") String time) {
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public String getName() {
        return name;
    }

    public long getAmount() {
        return amount;
    }

    public String getTime() {
        return time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BankTransaction that = (BankTransaction) o;
        return amount == that.amount &&
                Objects.equals(name, that.name) &&
                Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, amount, time);
    }

    @Override
    public String toString() {
        return "BankTransaction{" +
                "name='" + name + '\'' +
                ", amount=" + amount +
                ", time=" + time +
                '}';
    }
}
