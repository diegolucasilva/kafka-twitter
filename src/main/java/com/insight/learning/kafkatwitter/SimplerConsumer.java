package com.insight.learning.kafkatwitter;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class SimplerConsumer {
    private static Logger logger = LoggerFactory.getLogger(SimplerConsumer.class.getName());
    private static String GROUP_ID = "simple-consumer-group";
    private static String BOOTSTRAP_SERVER = "localhost:9092";
    private static String TOPIC = "first_topic";

    public static void main(String... args){
        //If you run more then one consumer, it'll be attach to specific partition:
        //[Consumer clientId=consumer-1, groupId=simple-consumer-group] Setting newly assigned partitions [first_topic-2]

        KafkaConsumer<String, String> consumer = configConsumer();
        //readFromKafka(consumer, TOPIC);

        fetchDataFromSpecifTopic(TOPIC,0,consumer);


    }

    private static KafkaConsumer<String, String> configConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //latest = only the new messages

        return new KafkaConsumer<>(properties);
    }

    private static void readFromKafka(Consumer consumer, String topic) {
        consumer.subscribe(Collections.singletonList(topic));

        while (true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100)); //100 milesconds to try get the data = timeout

            for(ConsumerRecord<String, String> record : records){
                SimplerConsumer.logger.info("Key: " + record.key() + " Value: "+record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");
            }
        }
    }

    private static void fetchDataFromSpecifTopic(String topic, int partition, Consumer consumer) {
        //assign and seek are mostly used to replay data or fetch a specific message
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        long offsetToReadFrom = 15;
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seek(topicPartition, offsetToReadFrom);

        int numberOfMessagesToRead = 2;

        while (numberOfMessagesToRead > 0){
            ConsumerRecords<String,String> recordss= consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : recordss){
                logger.info("Key: " + record.key() + " Value: "+record.value() + "\n" +
                        "Partition: " + record.partition() + "\n" +
                        "Offset: " + record.offset() + "\n");
                numberOfMessagesToRead--;
            }

        }
    }
}
