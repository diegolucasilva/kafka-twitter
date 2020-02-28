package com.insight.learning.kafkatwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private final String  consumerKey= "Dyyk5QJyPRkyxdAYEgTKgCRLg";
    private final String  consumerSecret= "LzNOS9gKnJUyL9wueD2XfT42Bi4fJtL5OH3papoJGiC7mlMOOy";
    private final String  token= "4409106880-xKItjP2Xzr3vHKTynjEEju28osY74cWr6QCcsyo";
    private final String  secret= "4271kBrUsWAp4fgSfoBDW1IMM19IyMbYeTsKgK0rqEVIG";

    public static void main(String... args){
        System.out.println("hello word");
        TwitterProducer twitterProducer = new TwitterProducer();
        twitterProducer.run();
    }

    public void run(){

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000); //size of message
        msgQueue.size();
        Client client =createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String>  producer = createProducer();
        while (!client.isDone()) { //get the messages in real time
            String msg = null;
            try {
                msg = msgQueue.poll(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info("message length " + msg.length());
                logger.info("message " + msg);

                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("error!", exception);
                        }
                    }
                });
            }
        }

        shutDown(producer, client);
    }

    private void shutDown(KafkaProducer<String, String> producer, Client client){
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
                client.stop();
                producer.close();
        }
        ));
    }

    private Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("usa","bitcoin");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();

    }

    public KafkaProducer<String, String> createProducer(){
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create a safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); //this config will enable the configs above automatically
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); //Kafka >= 1.1 = 5, otherwise = 1 (to guarantee order into the partition)

        //high throughput producer(at the expense of a bit of latency and CPU usages)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); //32kb;


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        return producer;

    }

}
