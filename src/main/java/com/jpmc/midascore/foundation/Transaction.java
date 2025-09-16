package com.jpmc.midascore.foundation;

public class Transaction {

    private long senderId;
    private long recipientId;
    private double amount;

    // Default constructor (needed for deserialization)
    public Transaction() {}

    // Constructor to create a Transaction with values
    public Transaction(long senderId, long recipientId, double amount) {
        this.senderId = senderId;
        this.recipientId = recipientId;
        this.amount = amount;
    }

    // Getters and setters
    public long getSenderId() {
        return senderId;
    }

    public void setSenderId(long senderId) {
        this.senderId = senderId;
    }

    public long getRecipientId() {
        return recipientId;
    }

    public void setRecipientId(long recipientId) {
        this.recipientId = recipientId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "Transaction {senderId=" + senderId + ", recipientId=" + recipientId + ", amount=" + amount + "}";
    }
}
