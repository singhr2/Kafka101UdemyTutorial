package com.github.singhr2.streaming;

/*
    This program will filter the tweets that are present in ElasticSearch(Bonsai.io)
    If there are no tweets available, please run the TwitterProducer program
    after setting the TWITTER_SEARCH_TERM / TWITTER_SEARCH_TERMS

    <TODO> verify the tweeter API credentials are valid.
    <TODO> Topics initial and Filtered must exists before running the program.
    To Create a topic:
    C:/> kafka-topics --zookeeper 127.0.0.1:2181 --create --topic filtered_tweets  --partitions 3 --replication-factor 1

 */

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class FilterTweetsUsingKafkaStreams {
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTweetsUsingKafkaStreams.class.getName());

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String STREAM_PROCESSING_APP_ID = "demo-kafka-streams";
    private static final String KAFKA_TOPIC_SUBSCRIBED = "twitter_tweets";
    private static final String KAFKA_TOPIC_FILTERED_TWEETS = "filtered_tweets"; // five thousand followers
    private static final int MINIMUM_FOLLOWERS = 3000;

    public static void main(String[] args) {
        System.out.println("..... Starting program to filter tweets...");

        //CREATE PROPERTIES
        Properties props = setStreamingConfiguration();

        //create a topology
        StreamsBuilder streamBuilder = new StreamsBuilder();

        //Filter the tweets from users which atleast 5000 followers
        KStream<String, String> inputStream = streamBuilder.stream(KAFKA_TOPIC_SUBSCRIBED);
        KStream<String, String> filteredStream = inputStream.filter(
            //filter only tweets where followers_count >= MINIMUM_FOLLOWERS
            (tweet_key, tweetAsJSON) -> getFollowersCount(tweetAsJSON) > MINIMUM_FOLLOWERS
        );

        filteredStream.to(KAFKA_TOPIC_FILTERED_TWEETS);

        //build the topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamBuilder.build(), props);

        //start our streams application
        kafkaStreams.start();
    }

    private static Properties setStreamingConfiguration() {
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, STREAM_PROCESSING_APP_ID);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        return props;
    }

    private static Integer getFollowersCount(String tweetDataAsJSON) {

        //LOGGER.info("tweetDataAsJSON :" + tweetDataAsJSON);

        JsonParser jsonParser = new JsonParser();
        Integer followersCount = 0;

        try{
            // user and followers_count are nodes in twitter response json
            followersCount = jsonParser.parse(tweetDataAsJSON).getAsJsonObject()
                    .get("user").getAsJsonObject()
                    .get("followers_count").getAsInt();
        } catch(NumberFormatException | NullPointerException e){
            LOGGER.warn("No. of followers is not a valid number" );
        }

        //LOGGER.info("--->>> followersCount :" + followersCount);

        return followersCount;
    }
}
