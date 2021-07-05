package com.kafka;

import com.services.KafkaConsumerService;
import com.services.KafkaProducerService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.concurrent.TimeUnit;

@SpringBootTest
@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@EmbeddedKafka(partitions = 1, brokerProperties = { "listeners=PLAINTEXT://${kafka.bootstrap.address}:${kafka.bootstrap.port}"})
//@Import({
//        com.kafka.config.KafkaConsumerConfig.class,
//        com.kafka.config.KafkaProducerConfig.class,
//        com.kafka.config.KafkaTopicConfig.class,
//})
public class EmbeddedKafkaTest {

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    @Test
    public void test() throws InterruptedException {
        kafkaProducerService.sendMessage("message");
        kafkaConsumerService.getLatch().await(10000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(kafkaConsumerService.getLatch().getCount(), 0L);
        Assert.assertEquals(kafkaConsumerService.getPayload(), "message");
    }
}
