package com.learn.kafka.realtime;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.BasicClient;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    ArrayList<String> terms = Lists.newArrayList("mannkibaat");


    public TwitterProducer() {

    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        BasicClient client = createTwitterClient(msgQueue);

        KafkaProducer<String, String> kafkaProducer = createKafkaProducer();
        client.connect();

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                kafkaProducer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e  != null) {
                            logger.error("Something bad happened" + e);
                        }
                    }
                });
            }
        }

        logger.info("End of application");


    }

    private KafkaProducer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true" );
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all" );
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5" );
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    private BasicClient createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */

/** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
// Optional: set up some followings and track terms
        hosebirdEndpoint.trackTerms(terms);

// These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1("53S20N2lmMD4pnWydhnhGfI6G", "oDG1vBaonZy2eh1ebqF0du0oC06OWqIdZyxgDZMIhnhzuBmO5C", "83569893-kvQ8YHSajR6iksNlyAFFGyvLOLnIhUViNUhRirSyi", "wdoByQOZZCLBHrvGjRK0QkKBJ8n3OkoXjr1lZWjtAQoFU");


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
