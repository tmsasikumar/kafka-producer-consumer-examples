package com.learn.kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallback {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
         System.out.println("Hello world");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i=1; i < 16; i++) {
            String topic_name = "first_topic";
            String value = "From java -" + i;
            String key = "id_" + i;

            //id1 - partition 0
            //id2 - partition 2
            //id3 - partition 0
            //id4 - partition 2
            //id5 - partition 2

            logger.info("key is " + key);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic_name, key,value);

            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("The exception is" + e.getMessage());
                    } else {
                        logger.info("Details of record inserted");
                        logger.info(
                                "Time stamp" + recordMetadata.timestamp() + "\n" +
                                        "Offset" + recordMetadata.offset() + "\n" +
                                        "Partition" + recordMetadata.partition() + "\n" +
                                        "Topic" + recordMetadata.topic() + "\n"
                        );
                    }
                }
            }).get();

        }
        producer.flush();
        producer.close();
    }
}
