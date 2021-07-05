package com.services;

import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@Service
public class KafkaConsumerService implements ConsumerSeekAware {

    private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);

    private CountDownLatch latch = new CountDownLatch(1);
    private String payload = null;
    private static final int partition = 0;


    @Value(value = "${kafka.topicName}")
    private String topicName;


    @KafkaListener(topics = "${kafka.topicName}", groupId = "${kafka.groupId}")
    public void listenGroupFoo(@Payload String message,
                               Acknowledgment acknowledgment,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName,
                               @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partitionId
                               ) {
        logger.info("Received Message {} on topic: {}, partitionId: {} ",message, topicName , partitionId);
        payload = message;
        latch.countDown();
        acknowledgment.acknowledge();
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
//        callback.seek(topicName, partition, 2);
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

    public String getPayload() {
        return payload;
    }
}
