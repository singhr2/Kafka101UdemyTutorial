package kaffka.tutorial2;

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
import java.util.Scanner;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Code reference: https://github.com/twitter/hbc
 * Documentation reference: https://docs.confluent.io/current/installation/configuration/producer-configs.html
 */
public class TwitterProducer {

    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    // These secrets should be read from a config file
    //Check/Regenerate new values from https://developer.twitter.com/ , https://developer.twitter.com/en/apps/18049294
    //TODO Update these values from Twitter App
    private static final String TWITTER_AUTHN_CONSUMER_API_KEY = "replace-with-consumer-key";
    private static final String TWITTER_AUTHN_CONSUMER_API_SECRET_KEY = "replace-with-sec-key";
    private static final String TWITTER_AUTHN_ACCESS_TOKEN = "replace-with-access-token";
    private static final String TWITTER_AUTHN_ACCESS_TOKEN_SECRET = "replace-with-secret";
    private static final Authentication TWITTER_AUTHN = new OAuth1(TWITTER_AUTHN_CONSUMER_API_KEY, TWITTER_AUTHN_CONSUMER_API_SECRET_KEY, TWITTER_AUTHN_ACCESS_TOKEN, TWITTER_AUTHN_ACCESS_TOKEN_SECRET);

    //TODO Update the text to search here
    private static final String TWITTER_SEARCH_TERM = "nda"; // bitcoin, palampur
    private static final String[] TWITTER_SEARCH_TERMS = {"cloud","kafka", "gbp"};
    //private static final String[] TWITTER_SEARCH_TERMS = {"palampur","kullu", "shimla" };

    private static final int QUEUE_CAPACITY = 1000;
    private static final String CLIENT_NAME = "Hosebird-Client-01";
    private static final int POLL_FREQUENCY = 50;  //millis

    //============ kafka related constants
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    //TODO Set the topic , this will be used by consumer groups for reading
    private static final String TOPIC_NAME = "twitter_tweets";
    private static final String ACKS_CONFIG_ALL_IN_SYNC_REPLICAS = "all";
    private static final String MAX_IN_FLIGHT_REQUEST_PER_BROKER = "5";
    private static final int BATCH_SIZE_IN_BYTES = 32 * 1024;
    private static final String COMPRESSION_TYPE_SNAPPY = "snappy";
    private static final int LATENCY_IN_SENDING_RECORDS = 200; // millis


    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
//    private /*static final*/ BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(QUEUE_CAPACITY);

    //commenting as we don't want to handle events at the moment
    // BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    private /*static final*/ Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    private /*static final*/ StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

    //constructor
    public TwitterProducer(){};

    public static void main(String[] args) {
        new TwitterProducer().searchForTweets();
    }

    public void searchForTweets(){ //run
        logger.info("Starting...");

        logger.info("* ************************************** *");
        logger.info("*           !!!! IMPORTANT !!!!           ");
        logger.info("* ENSURE THAT TWITTER API KEY and         ");
        logger.info("* ACCESS TOKEN are configured correctly   ");
        logger.info("* ");
        //logger.info("* Press any text and hit ENTER to continue..........");
        logger.info("* ************************************** *");

        //This is added just to remind before running to verify the twitter API details
        /*
        Scanner in = new Scanner(System.in);  //InputStream
        String ack = in.next(); // string input
        */

        //introducing sleep to read the above message
        // In case this program is run after long time.
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(QUEUE_CAPACITY);

        // 1. create Twitter client
        Client twitterClient  = configureAndCreateTwitterClient(msgQueue);

        // Attempts to establish a connection.
        twitterClient.connect();

        // 2. Create a Kafka producer
        Properties kafkaProps = setKafkaProperties();
        KafkaProducer<String, String> producer = new KafkaProducer(kafkaProps);

        //Note: !!! PRE-REQUISITE !!!
        //run the following command from CLI to create the required topic
        // kafka-topics --zookeeper 127.0.0.1:2181 --create --topic twitter-tweets  --partitions 6 --replication-factor 1
        //

        // 3. loop to send tweets to Kaffa cluster
        readFromQueues(msgQueue, twitterClient, producer);
    }



    private Client configureAndCreateTwitterClient(BlockingQueue<String> msgQueue) {
//        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
//        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        //===========================================================

        //Either we will follow people or terms.

        //List<String> term = Lists.newArrayList(TWITTER_SEARCH_TERM);
        //hosebirdEndpoint.trackTerms(term);

        List<String> terms = Lists.newArrayList(TWITTER_SEARCH_TERMS);
        hosebirdEndpoint.trackTerms(terms);

        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        //hosebirdEndpoint.followings(followings);

        //===========================================================

        //moved to top
        // Authentication TWITTER_AUTHN = new OAuth1(TWITTER_AUTHN_CONSUMER_API_KEY, TWITTER_AUTHN_CONSUMER_API_SECRET_KEY, TWITTER_AUTHN_ACCESS_TOKEN, TWITTER_AUTHN_ACCESS_TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder() // GoF-Creational-Builder pattern usage
                .name(CLIENT_NAME)  // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(TWITTER_AUTHN)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue); // optional: use this if you want to process client events

        Client tmpHosebirdClient = builder.build();

        return tmpHosebirdClient;
    }

    private void readFromQueues(BlockingQueue<String> msgQueue, Client client, KafkaProducer<String, String> producer) {
        ProducerRecord producerRecord;

        // on a different thread, or multiple different threads....
        logger.info("Starting to read tweets...");

        while (!client.isDone()) {
            String msgRead = null;

            try {
                msgRead = msgQueue.poll(POLL_FREQUENCY, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                //logger.error( e.getMessage(), e);
                client.stop();
            }

            if(msgRead != null) {
                logger.info(">>" + msgRead);

                // If message was read, use Kafka producer to send to Kafka brokers
                producerRecord = new ProducerRecord(TOPIC_NAME, TWITTER_SEARCH_TERM, msgRead);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata rm, Exception e) {
                        if(e!=null){
                            logger.error("Some error occured", e);
                        } else {
                            logger.info("Message added to topic ["+ TOPIC_NAME+"], \n" +
                                    "partition: ["+ rm.partition()+"], \n" +
                                    "offset: [" + rm.offset() + "], \n" +
                                    "Timestamp: " + rm.timestamp());
                        }
                    }
                });
            }

            logger.info("end of application.");
        }
    }

    private static Properties setKafkaProperties() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //control acknowledgment, retries, idempotent, max in-flight requests
        configurePropertiesForSafeProducer(props);

        // control batching, compression-type and linger time for high-performance
        configurePropertiesForHighPerformance(props);

        return props;

    }

    private static Properties configurePropertiesForSafeProducer(Properties props) {
        //These setting for required ONLY for safe (idempotent) producer
        //MANDATORY
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //THESE ARE IMPLICIT and not required to be set BUT MENTIONED HERE EXPLICITLY FOR CLARITY
        props.setProperty(ProducerConfig.ACKS_CONFIG, ACKS_CONFIG_ALL_IN_SYNC_REPLICAS);
        props.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));


        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, MAX_IN_FLIGHT_REQUEST_PER_BROKER); // only for Kafka >= 1.0
        return props;
    }


    private static void configurePropertiesForHighPerformance(Properties props) {
        //The producer will attempt to batch records together into fewer requests
        // whenever multiple records are being sent to the same partition.
        // This helps performance on both the client and the server.
        // This configuration controls the default batch size in bytes.

        //No attempt will be made to batch records larger than this size.

        //Requests sent to brokers will contain multiple batches,
        // one for each partition with data available to be sent.
        //A small batch size will make batching less common and may reduce throughput
        // (a batch size of zero will disable batching entirely).
        // A very large batch size may use memory a bit more wastefully
        // as we will always allocate a buffer of the specified batch size
        // in anticipation of additional records.

        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(BATCH_SIZE_IN_BYTES)); // 32 Mb

        //The compression type for all data generated by the producer.
        // The default is none (i.e. no compression).
        // Valid values are none, gzip, snappy, lz4, or zstd.
        // Compression is of full batches of data, so the efficacy of batching
        // will also impact the compression ratio (more batching means better compression).

        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, COMPRESSION_TYPE_SNAPPY); //compression type


        // defaults to 0 (i.e. no delay)
        // This setting gives the upper bound on the delay for batching:
        // once we get batch.size worth of records for a partition it will be sent immediately
        // regardless of this setting, however if we have fewer than this many bytes accumulated
        // for this partition we will 'linger' for the specified time waiting for more records to show up.
        // This setting defaults to 0 (i.e. no delay).
        // Setting linger.ms=5, for example, would have the effect of reducing the number of requests sent
        // but would add up to 5ms of latency to records sent in the absence of load.
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, Integer.toString(LATENCY_IN_SENDING_RECORDS));
    }
}
