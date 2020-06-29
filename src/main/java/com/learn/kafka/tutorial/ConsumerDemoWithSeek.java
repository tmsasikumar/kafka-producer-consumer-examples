package com.learn.kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ConsumerDemoWithSeek {

    public static void main(String[] args) throws ExecutionException {
        final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithSeek.class);
         System.out.println("Hello world");

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        TopicPartition topicPartition = new TopicPartition("first_topic", 0);
        long offSetToStart = 5L;
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offSetToStart);
       // consumer.subscribe(Arrays.asList("first_topic"));

        int numberOfMessagesToRead = 2;
        int messagesRead = 0;
        boolean shouldContinue =  true;
        while (shouldContinue) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord record : records) {
                messagesRead++;
                logger.info("Key:" + record.key() + "Value:" + record.value());
                logger.info("Partition:" + record.partition() + "Offset:" + record.offset());
                if(messagesRead >= numberOfMessagesToRead)
                {
                    shouldContinue = false;
                    break;
                }


            }
        }
    }
}
