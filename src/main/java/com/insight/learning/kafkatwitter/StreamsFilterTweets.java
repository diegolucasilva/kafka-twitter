package com.insight.learning.kafkatwitter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    private static String BOOTSTRAP_SERVER = "localhost:9092";

    private static JsonParser jsonParser = new JsonParser();

    public static void main(String... args){

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputTopic = builder.stream("twitter_tweets");

        filterImportantTweets(inputTopic);

        Properties properties = configProperties();
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        kafkaStreams.start();
    }

    private static void filterImportantTweets(KStream<String, String> inputTopic) {
        inputTopic.filter(
                (k, jsonTweets) -> extractUserFollowersInTweet(jsonTweets) > 1000
        ).to("important_tweets");
    }


    private static Properties configProperties() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return properties;
    }

    private static int extractUserFollowersInTweet(String value) {
        try {
            return jsonParser.parse(value).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
        }catch (Exception e){
            return 0;
        }
    }
}
