package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.stereotype.Component;

@Component
public class BalanceQuerier {
    private final UserRepository userRepository;

    public BalanceQuerier(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public float getBalance(long userId) {
        return userRepository.findById(userId)
                .map(UserRecord::getBalance)
                .orElseThrow(() -> new RuntimeException("User not found with id " + userId));
    }
}
