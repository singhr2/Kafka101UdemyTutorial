package com.github.singhr2.kafka.turorial3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

/**
 * Pre-requisites:
 * 1) Zookeeper is running
 * 2) Kafka Server / broker(s) are running
 * *) Ensure data is added to the topic in Kafka,
 * these records will be fetched in ElasticSearch from this Topic.
 *      If required, set the keyword to search in TwitterProducer class
 *      and run the TwitterProducer'
 * 3) ElasticSearch (Bonsai) Index exists in ElasticSearch/Bonsai.io
 * PUT /your-index-name
 * 4) Credentials for Kafka and Bonsai.io are configured correctly
 * 5)
 */

/*
 References:
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-index.html#java-rest-high-document-index-sync
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-document-get.html
 * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/master/java-rest-high-create-index.html
 * https://medium.com/faun/java-high-level-rest-client-elasticsearch-7-be4fb247c286
 * https://medium.com/@ashish_fagna/getting-started-with-elasticsearch-creating-indices-inserting-values-and-retrieving-data-e3122e9b12c6
 * https://elasticsearch-cheatsheet.jolicode.com/
 * https://www.baeldung.com/elasticsearch-java
 * https://www.huaweichen.com/2019/kafka-05-kafka-consumer-with-elasticsearch/
 * https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/ch04.html
 *
 *
 */
public class ElasticSearchConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    //########### Bonsai.io ###########
    // Full Access URL
    //https://ljo0h46kpu:hwx2w7foud@kafka-course-849445253.ap-southeast-2.bonsaisearch.net:443
    //scheme://username:pwd@hostname:port
    private static final String SCHEME_NAME = "https";
    private static final String HOST_NAME = "kafka-course-849445253.ap-southeast-2.bonsaisearch.net";
    private static final String PORT_NUMBER = "443";
    private static final String USER_NAME = "ljo0h46kpu";
    private static final String PASSWORD = "hwx2w7foud";

    //########### KAFKA CONSUMER related ###########
    private static final String KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KAFKA_CONSUMER_GROUP_ID = "kafka-demo-elasticsearch";
    private static final String AUTO_COMMIT_OFFSETS = "false";
    private static final String MAX_RECORDS_TO_POLL = "5"; //lower number suitable only for testing.

    //Ensure that this topic exists, else following error will be reported and topic with default values will be created.
    //  [main] WARN org.apache.kafka.clients.NetworkClient - [Consumer clientId=consumer-1, groupId=kafka-demo-elasticsearch]
    //  Error while fetching metadata with correlation id 8 : {zzzzzz=LEADER_NOT_AVAILABLE}
    private static final String KAFKA_TOPIC_SUBSCRIBED = "twitter_tweets";

    //############ ElasticSearch related ###########
    private static final String ELASTIC_SEARCH_INDEX = "twitter_index";// ~ Database
    public static final String TWEET_ATTRIBUTE_FOR_ID_GENERATION = "id_str";
    //private static final String ELASTIC_SEARCH_TYPES = "tweets"; // ~ Table (deprecated in ES 6.0)

    public static void main(String[] args) {
        LOGGER.info("Hello .... from ElasticSearch client....");

        ConsumerRecords<String, String> batchOfRecords = null;

        // COMMENT FOR BATCH PROCESSING
        IndexResponse indexResponse; // = null;
        BulkResponse bulkResponse; // only for Bulk processing

        //Create ElasticSearch client
        RestHighLevelClient rhlc = createElasticClient();

        //Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer(KAFKA_TOPIC_SUBSCRIBED);

        try {
            //poll the topic for new data
            while (true) {
                //Consumer will poll the subscribed twitter topic
                batchOfRecords = kafkaConsumer.poll(Duration.ofMillis(100));
                LOGGER.info("--> Total records fetched :" + batchOfRecords.count());

                if (batchOfRecords.count() > 0) {
                    // ADDED ONLY FOR BATCH PROCESSING
                    BulkRequest bulkRequest = new BulkRequest();

                    // INSERT DATA INTO ELASTICSEARCH
                    for (ConsumerRecord<String, String> record : batchOfRecords) {
                        //UNCOMMENT if indexing individual records
                        //indexResponse = indexDocuments(rhlc, record, bulkRequest);

                        //UNCOMMENT WHEN USING BulkRequest, this will add the current request to BulkRequest
                        indexDocuments(rhlc, record, bulkRequest);
                    } // for-loop

                    // FOR BULK REQUESTS ONLY
                    bulkResponse = rhlc.bulk(bulkRequest, RequestOptions.DEFAULT);

                    //TODO Remove after testing
                    //Thread.sleep(1000); // optional; to introduce delay
                } else {// records found
                    LOGGER.info("No tweets(documeents) found..");
                    //Note: You can verify that all records have been processed by executing following kafka command:
                    //$ kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group kafka-demo-elasticsearch --describe
                    //Note: kafka-demo-elasticsearch is the name of the consumer group set in this class.
                }

                //Do it only if ENABLE_AUTO_COMMIT_CONFIG="false"
                manuallyCommitOffset(kafkaConsumer);

                //TODO This is optional and added just to give time
                Thread.sleep(1000);
            }
        } catch (InterruptedException | IOException e) {
            //e.printStackTrace();
            LOGGER.error("Exception occured", e);
        }

        // ***** Querying Indexed Documents *****
        //searchDocuments(rhlc, id);

        // ***** DELETE DOCUMENTS *****
        //deleteDocuments(rhlc, id);

        //Close client gracefully.
        //rhlc.close();
    }

    private static void manuallyCommitOffset(KafkaConsumer<String, String> kafkaConsumer) {
        //---------------------------------------------------
        //<IMPORTANT> *** Manual Offset Control ***
        //---------------------------------------------------
        // We need to manually commit offset if ENABLE_AUTO_COMMIT_CONFIG="false"
        // Instead of relying on the consumer to periodically commit consumed offsets, users can also
        // control when records should be considered as consumed and hence commit their offsets.
        // This is useful when the consumption of the messages is coupled with some processing logic
        // and hence a message should not be considered as consumed until it is completed processing.

        LOGGER.info("...............................Before committing offsets....");
        //Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
        kafkaConsumer.commitSync();
        LOGGER.info("................................. Offsets have been commited");
    }

    private static RestHighLevelClient createElasticClient() {
        //
        // Ref: https://docs.bonsai.io/article/278-java

        //This is only required because of using managed ElasticSearch instance in bonsai.io
        //not required if using local instance
        final CredentialsProvider credProvider = new BasicCredentialsProvider();
        credProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(USER_NAME, PASSWORD));

        HttpHost host = new HttpHost(HOST_NAME, Integer.parseInt(PORT_NUMBER), SCHEME_NAME);

        RestClientBuilder builder = RestClient.builder(host)
                .setHttpClientConfigCallback(
                        httpAsyncClientBuilder ->
                                httpAsyncClientBuilder.setDefaultCredentialsProvider(credProvider)
                                        .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                );

        RestHighLevelClient rhlc = new RestHighLevelClient(builder);

        System.out.println(" created client...");
        return rhlc;
    }


    private static Properties setKafkaConsumerConfiguration() {
        // Ref: https://kafka.apache.org/20/documentation.html#newconsumerconfigs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

         /*
            Kafka uses the concept of consumer groups to allow a pool of processes to divide the work
            of consuming and processing records. These processes can either be running on the same machine
            or they can be distributed over many machines to provide scalability and fault tolerance
            for processing. All consumer instances sharing the same group.id will be part of the same consumer group.
         */
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KAFKA_CONSUMER_GROUP_ID);

        /*
            The Offset is a piece of metadata, an integer value that continually increases for each message
            that is received in a partition. Each message will have a unique Offset value in a partition.
            Each message has a unique Offset, and that Offset represents the position of that message in that particular partition.

            When a Consumer reads the messages from the Partition it lets Kafka know the Offset
            of the last consumed message. This Offset is stored in a Topic named _consumer_offsets,
            in doing this a consumer can stop and restart without forgetting which messages it has consumed.
         */

        //earliest: automatically reset the offset to the earliest offset
        //latest: automatically reset the offset to the latest offset
        //none: throw exception to the consumer if no previous offset is found for the consumer's group
        //anything else: throw exception to the consumer.
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest=from-begining??

        //{OPTIONAL} to disable auto-commit of offsets
        // we need to manually commit the offset if it is set to false
        //If true the consumer's offset will be periodically committed in the background.
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, AUTO_COMMIT_OFFSETS);

        //The maximum number of records returned in a single call to poll().
        //Keep it to low number to do testing
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_RECORDS_TO_POLL);

        return props;
    }


    private static KafkaConsumer<String, String> createKafkaConsumer(String topicSubscribed) {
        //1. create consumer config
        Properties props = setKafkaConsumerConfiguration();

        // create consumer
        KafkaConsumer<String, String> _KafkaConsumer = new KafkaConsumer(props);

        //subscribe consumer to topic(s)
        //consumer.subscribe(Collections.singletonList(TOPIC_SUBSCRIBED));
        _KafkaConsumer.subscribe(Arrays.asList(topicSubscribed));

        return _KafkaConsumer;
    }

    /**
     * Note: The index should already be created in ElasticSearch
     * else error is returned e.g.,
     * {"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [xxx]
     * and [action.auto_create_index] ([*logstash*,*requests*,*events*,*.kibana*,...,*grax*])
     * doesn't match","index_uuid":"_na_","index":"xxx"},"status":404}
     * <p>
     * Solution: Create index before hand by executing below command
     * Request:
     * PUT /your-index-name-here
     * Response:
     * {
     * "acknowledged": true,
     * "shards_acknowledged": true,
     * "index": "twitter_index"
     * }
     **/
    //TODO Split this to handle Bulk vs individual record
    private static /*IndexResponse*/ void indexDocuments(RestHighLevelClient rhlc, ConsumerRecord<String, String> rec, BulkRequest bulkRqst)
            throws IOException {
        //IndexResponse response = null;

        // ***** save a document in Elasticsearch *****
        IndexRequest indexRequest = new IndexRequest(ELASTIC_SEARCH_INDEX); // database

        //We need to set a unique-id to each document to avoid duplicates
        //TODO It might throw NPE if id_str is not present in twitter response.
        //in case of NPE, we may decide to skip the record for processing
        setUniqueIdForDocument(indexRequest, rec);

        //<<<< START----------------------------------------------
        // indexRequest.source(jsonInputString -OR- jsonInputXContentBuilder )

        LOGGER.info("<!!!> Record: " + rec.toString()); // print all attributes
        LOGGER.info("<!!!> Note: We are just storing the value in ElasticSearch.");

        //(a)
        String jsonInputString = rec.value();
        LOGGER.info(">> rec.value() :" + rec.value());
        //String jsonInputString = populateDocumentAttributesAsString(); // for dummy data
        indexRequest.source(jsonInputString, XContentType.JSON);

        //(b)
        // XContentBuilder jsonInputXContentBuilder = populateDocumentAttributesAsXContentBuilder(); // for dummy data
        // indexRequest.source(jsonInputXContentBuilder  /*, XContentType.JSON*/);

        //--------------------------------------------END >>>>


        //COMMENT for bulk processing
        //Synchronous execution (one record at a time) -
        //response = rhlc.index(indexRequest, RequestOptions.DEFAULT);

        //COMMENT if individual record processing
        bulkRqst.add(indexRequest);

        //Fetch details of record received.
//        LOGGER.info("response: " + response);
//        LOGGER.info("id :" + response.getId() + ", version: " + response.getVersion() + ", index: " + response.getIndex());
//        LOGGER.info("IndexResponse: " + response.getResult());

//        return response;
    }


    /**
     *  Idempotent Stategy - this is to make our consumer idempotent
     *
     * @param indexRequest
     * @param rec
     */
    private static void setUniqueIdForDocument(IndexRequest indexRequest, ConsumerRecord<String, String> rec) {
        String uuidForDocument = "";

        //[Option-1] : Generate a UUID - but this will not prevent duplicate record entry as it time it will get a new value
        //uuidForDocument = UUID.randomUUID().toString(); // Row-id = document-id
        //LOGGER.info("uuidForDocument - generated :" + uuidForDocument);

        //[Option-2] : Generate Kafka Generic-ID
        //uuidForDocument = rec.topic() + "" + rec.partition() + "" + rec.offset() ;
        //LOGGER.info("uuidForDocument from Kafka Generic-ID :" + uuidForDocument);

        //[Option-3] : Get some unique-id provided by Twitter
        // We need to parse the JSON received fom twitter using library like google/gson
        // gson - A Java serialization/deserialization library to convert Java Objects into JSON and back
        // Note: the method is deprecated in 2.8.6
        JsonParser jsonParser = new JsonParser();

        //TODO Set the field from response which can be used to avoid duplicate records
        // here we are using 'id_str' but might need to change
        uuidForDocument = jsonParser.parse(rec.value()).getAsJsonObject().get(TWEET_ATTRIBUTE_FOR_ID_GENERATION).getAsString();
        LOGGER.info("uuidForDocument from tweet's id_str field :" + uuidForDocument);

        //finally, set the value
        indexRequest.id(uuidForDocument);
    }


    private static void searchDocuments(RestHighLevelClient rhlc, String id) throws IOException {
        // GET  /twitter_index/_search?q=_id:74d56660-5429-4f7e-91e7-fecd800c87c9
        GetRequest getRequest = new GetRequest(ELASTIC_SEARCH_INDEX, id);
        GetResponse getResponse = rhlc.get(getRequest, RequestOptions.DEFAULT);
        //String message = getResponse.getField("message").getValue();
        LOGGER.info("GetResponse: " + getResponse);
    }


    //todo Verify implementation
    private static void deleteDocuments(RestHighLevelClient rhlc, String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest("your-index-name-here");
        deleteRequest.id(id);
        DeleteResponse deleteResponse = rhlc.delete(deleteRequest, RequestOptions.DEFAULT);
    }


    //TODO This is dummy implementation, replace with actual values
    private static XContentBuilder populateDocumentAttributesAsXContentBuilder() throws IOException {
        //Document source provided as an XContentBuilder object,
        // the Elasticsearch built-in helpers to generate JSON content
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("author", "ranbir");
            //builder.timeField("postDate", new Date());
            builder.field("topic", "kafka");
            builder.field("message", "xperimenting with kafka and elasticsearch");

        }
        builder.endObject();

        return builder;
    }


    //TODO This is dummy implementation, replace with actual values
    private static String populateDocumentAttributesAsString() throws IOException {
        // JSON Creation: https://tools.knowledgewalls.com/jsontostring
        // Formatting: https://jsonformatter.curiousconcept.com/

        return "{ " +
                "\"author\" : \"ranbir\" , " +
                "\"topic\":\"kafka\"," +
                "\"message\":\"experimenting with kafka and elasticsearch" +
                "\"}";
    }
}