package com.jpmc.midascore.component;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;

@Component
public class DatabaseConduit {
    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConduit.class);

    public DatabaseConduit(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    public void save(UserRecord userRecord) {
        userRepository.save(userRecord);
    }

    public void saveTransaction(TransactionRecord transactionRecord) {
        transactionRepository.save(transactionRecord);
    }

    public boolean processTransaction(Transaction transaction) {
        if (transaction == null) return false;
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());
        if (sender == null || recipient == null) {
            logger.info("Discarding transaction due to missing user: {} -> {}", transaction.getSenderId(), transaction.getRecipientId());
            return false;
        }
        if (sender.getBalance() < transaction.getAmount()) {
            logger.info("Discarding transaction due to insufficient funds: {} has {} needs {}", sender.getName(), sender.getBalance(), transaction.getAmount());
            return false;
        }

        // Deduct from sender and credit recipient with transaction amount
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());
        userRepository.save(sender);
        userRepository.save(recipient);

        // Call incentive API
        float incentiveAmount = 0f;
        try {
            org.springframework.web.client.RestTemplate rt = new org.springframework.web.client.RestTemplate();
            String url = "http://localhost:8080/incentive";
            // send the Transaction object directly; expect JSON {"amount": <num>}
            java.util.Map response = rt.postForObject(url, transaction, java.util.Map.class);
            if (response != null && response.get("amount") != null) {
                Object val = response.get("amount");
                if (val instanceof Number) {
                    incentiveAmount = ((Number) val).floatValue();
                } else {
                    incentiveAmount = Float.parseFloat(val.toString());
                }
            }
        } catch (Exception ex) {
            logger.warn("Failed to call incentive API: {}", ex.toString());
            incentiveAmount = 0f;
        }

        // Apply incentive to recipient (do not deduct sender)
        if (incentiveAmount > 0f) {
            recipient.setBalance(recipient.getBalance() + incentiveAmount);
            userRepository.save(recipient);
        }

        TransactionRecord tr = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(tr);
        logger.info("Processed transaction {} -> {} amount {} incentive {}. Sender balance={} Recipient balance={}", sender.getName(), recipient.getName(), transaction.getAmount(), incentiveAmount, sender.getBalance(), recipient.getBalance());
        return true;
    }

}
