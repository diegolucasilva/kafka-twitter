package com.insight.learning.kafkatwitter;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducer {

    //https://docs.confluent.io/current/installation/configuration/producer-configs.html

    private static final Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC = "first_topic";


    public static void main(String... args) throws ExecutionException, InterruptedException {

        KafkaProducer<String, String> producer = configProducer();

        produceMessages(producer);

    }

    private static void produceMessages(KafkaProducer<String, String> producer) throws InterruptedException, ExecutionException {
        //producer.send(record); // asynchronous by default

        for(int i = 0; i < 10; i++) {
            String key = "id_" + i;
            String value = "hello word "+ i;

           // ProducerRecord<String, String> record = new ProducerRecord("first_topic", "hello word " +i); //without key and callback

            ProducerRecord<String, String> record = new ProducerRecord(TOPIC, key, value);

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n");
                    } else
                        logger.error("Error " + exception);
                }
            }).get();  //force the .send() to make it synchronous - don't do this in production!
        }
        producer.flush();
        producer.close();
    }

    private static KafkaProducer<String, String> configProducer() {
        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(properties);
    }
}
