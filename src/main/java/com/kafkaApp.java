package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableAsync;

@SpringBootApplication
//@ComponentScan(basePackages = {
//        "com.kafka.config",
//        "com.kafka.services",
//})
@EnableAsync
public class kafkaApp {
    public static void main(String[] args) {
        SpringApplication.run(kafkaApp.class, args);
    }
}
