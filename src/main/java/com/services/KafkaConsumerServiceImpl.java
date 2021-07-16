package com.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.concurrent.CountDownLatch;

@Service
public class KafkaConsumerServiceImpl implements KafkaConsumerService, ConsumerSeekAware {

    private static final Logger logger = LogManager.getLogger(KafkaConsumerServiceImpl.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private String payload = null;
    private static final int partition = 0;
    private ConsumerSeekCallback callback;

    @Value(value = "${kafka.topicName}")
    private String topicName;


    @KafkaListener(topics = "${kafka.topicName}", groupId = "${kafka.groupId}")
    public void listenGroupFoo(@Payload String message,
                               @Header(KafkaHeaders.ACKNOWLEDGMENT) Acknowledgment acknowledgment,
                               @Header(KafkaHeaders.OFFSET) int offSet,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionId) {
        logger.warn("Received Message {} on topic: {}, partitionId: {} offSet={}", message, topicName, partitionId,offSet);
        payload = message;
        if (!message.equals("message2")) {
            acknowledgment.acknowledge();
        }
        latch.countDown();
    }

    @Override
    public CountDownLatch getLatch() {
        return latch;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.callback = callback;
    }

    @Override
    public void resetToOffset(int offsetNum) {
        callback.seek(topicName, partition, offsetNum);
    }
//
//    @Override
//    public  void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback){
//
//    }
//    @Override
//    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
//        System.out.println(assignments);
//       super.onPartitionsAssigned(assignments, callback);
//        callback.seekToBeginning(assignments.keySet());
//    }
//
//    public void seekToTime(long time) {
//        getSeekCallbacks().forEach((tp, callback) -> callback.seekToTimestamp(tp.topic(), tp.partition(), time));
//    }
//
//    public void seekToOffset(TopicPartition tp, long offset) {
//        getSeekCallbackFor(tp).seek(tp.topic(), tp.partition(), offset);
//    }

    @Override
    public String getPayload() {
        return payload;
    }
}
