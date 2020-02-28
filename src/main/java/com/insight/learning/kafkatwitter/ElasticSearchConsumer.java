package com.insight.learning.kafkatwitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
//add -Dlog4j.configuration=file:/Users/diegolucas/Desktop/estudo/kafka-twitter/src/main/resources/log4j.properties
public class ElasticSearchConsumer {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static JsonParser jsonParser = new JsonParser();

    public static final String hostName = "kafka-learning-8312723561.ap-southeast-2.bonsaisearch.net";
    public static final String userName = "7ggev9l7eg";
    public static final String password = "37n2vp5145";
    private static String GROUP_ID = "elastic-search-consumer";
    private static String BOOTSTRAP_SERVER = "localhost:9092";
    private static String TOPIC = "twitter_tweets";

    public static void main(String... args) throws IOException, InterruptedException {
        readFromKafka(TOPIC);

    }

    public static RestHighLevelClient createClient(){
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(userName,password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostName,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return  client;

    }


    private static void readFromKafka(String topic) throws IOException, InterruptedException {

        KafkaConsumer<String, String> consumer = configConsumer();
        consumer.subscribe(Collections.singletonList(topic));

        RestHighLevelClient client = createClient();

        while (true){
            ConsumerRecords<String,String> records= consumer.poll(Duration.ofMillis(100));
            logger.info("Received "+ records.count()+" records");
            if(records.count() > 0) {
                BulkRequest bulkRequest = new BulkRequest();

                for(ConsumerRecord<String, String> record : records){

                    // logger.info("\n" +"Value: "+record.value() + "\n" +"Partition: " + record.partition() + "\n" +"Offset: " + record.offset() + "\n");
                    try {
                        String id = extractIdFromTweet(record.value()); //To guarantee idempotent message. Could be too record.topic() + record.partition() + record-offset();
                        IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).
                                source(record.value(), XContentType.JSON);
                        bulkRequest.add(indexRequest);
                       // Thread.sleep(1);
                    }
                    catch(Exception e){
                        logger.info("skiped bad data " +record.value());
                    }
                }
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("bulk size " + bulkItemResponses.getItems().length);
                logger.info("Committing offsets...");
                consumer.commitSync();
                logger.info("Offesets have been committed...");
            }
        }
        //client.close();
    }

    private static String extractIdFromTweet(String value) {
        return jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
    }

    private static void writeElasticSearchOneByOne(RestHighLevelClient client, String message, String id) throws IOException {
        IndexRequest indexRequest =new IndexRequest("twitter", "tweets", id).
                source(message, XContentType.JSON);
         IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        logger.info("index id "+indexResponse.getId());
    }

    private static KafkaConsumer<String, String> configConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //? doesn't work. In the command line works //latest = only the new messages
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "10");
        return new KafkaConsumer<>(properties);
    }

}
