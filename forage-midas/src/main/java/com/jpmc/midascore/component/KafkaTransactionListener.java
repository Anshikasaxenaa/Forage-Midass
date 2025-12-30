package com.jpmc.midascore.component;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.foundation.Transaction;

@Component
public class KafkaTransactionListener {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    private final List<Transaction> received = new CopyOnWriteArrayList<>();
    private final DatabaseConduit databaseConduit;

    public KafkaTransactionListener(DatabaseConduit databaseConduit) {
        this.databaseConduit = databaseConduit;
    }

    @KafkaListener(topics = "${general.kafka-topic}", containerFactory = "transactionKafkaListenerContainerFactory")
    public void listen(Transaction transaction) {
        if (transaction == null) {
            logger.warn("Received null transaction");
            return;
        }
        received.add(transaction);
        logger.info("Received transaction #{}: {}", received.size(), transaction);
        boolean saved = databaseConduit.processTransaction(transaction);
        if (saved) {
            logger.info("Transaction saved to DB");
        } else {
            logger.info("Transaction discarded");
        }
    }

    public List<Transaction> getReceived() {
        return received;
    }
}
