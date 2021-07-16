package com.kafka;

import com.kafkaApp;
import com.services.KafkaConsumerService;
import com.services.KafkaConsumerServiceImpl;
import com.services.KafkaProducerService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Import({
        com.kafka.config.KafkaConsumerConfig.class,
        com.kafka.config.KafkaProducerConfig.class,
        com.kafka.config.KafkaTopicConfig.class,
})
@SpringBootTest(classes = kafkaApp.class)
public class KafkaContainersLiveTest {

    @ClassRule
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));


    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Autowired
    private KafkaConsumerService kafkaConsumerService;

    @Test
    public void test() throws InterruptedException {
        kafkaProducerService.sendMessage("message");
        kafkaConsumerService.getLatch().await(1000, TimeUnit.MILLISECONDS);

        Assert.assertEquals(kafkaConsumerService.getLatch().getCount(), 0L);
        Assert.assertEquals(kafkaConsumerService.getPayload(), "message");
    }
}
