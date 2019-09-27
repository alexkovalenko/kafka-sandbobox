package com.github.alexkovalenko.stream.bankbalance.dto;

import java.util.Objects;

public class BankBalance {
    private long transactionCounter;
    private String name;
    private long amount;
    private String time;

    public BankBalance() {
    }

    public BankBalance(long transactionCounter, String name, long amount, String time) {
        this.transactionCounter = transactionCounter;
        this.name = name;
        this.amount = amount;
        this.time = time;
    }

    public long getTransactionCounter() {
        return transactionCounter;
    }

    public void setTransactionCounter(long transactionCounter) {
        this.transactionCounter = transactionCounter;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BankBalance that = (BankBalance) o;
        return transactionCounter == that.transactionCounter &&
                amount == that.amount &&
                Objects.equals(name, that.name) &&
                Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(transactionCounter, name, amount, time);
    }

    @Override
    public String toString() {
        return "BankBalance{" +
                "transactionCounter=" + transactionCounter +
                ", name='" + name + '\'' +
                ", amount=" + amount +
                ", time='" + time + '\'' +
                '}';
    }
}
