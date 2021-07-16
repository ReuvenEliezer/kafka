package com.services;

import java.util.concurrent.CountDownLatch;

public interface KafkaConsumerService {

    void resetToOffset(int offsetNum);

    CountDownLatch getLatch();

    String getPayload();
}
