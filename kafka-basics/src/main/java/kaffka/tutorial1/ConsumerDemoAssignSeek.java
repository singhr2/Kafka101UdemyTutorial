package kaffka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * Here, we don't want to subscribe to any topic
 * we dont want to use the group-id as well
 *
 * AssignAndSeek is mostly used to replay data or
 * fetch a specific message
 */
public class ConsumerDemoAssignSeek {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    //private static final String CONSUMER_GROUP_ID = "my-first-application";
    private static final String TOPIC_SUBSCRIBED = "first_topic";
    private static final int TOPIC_PARTITION_TO_READ_FROM = 2;
    private static final long TOPIC_PARTITION_OFFSET_TO_READ_FROM = 15L; // starting offset
    private static final int NO_OF_MESSAGES_TO_BE_READ = 3;


    public static void main(String[] args) {

        //1. create consumer config
        Properties props = setConfiguration();

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //subscribe consumer to topic(s)
        //consumer.subscribe(Collections.singletonList(TOPIC_SUBSCRIBED));
        //consumer.subscribe(Arrays.asList(TOPIC_SUBSCRIBED));

        //assign And seek are used to replay data or fetch specific message
        TopicPartition partitionToReadFrom = new TopicPartition(TOPIC_SUBSCRIBED, TOPIC_PARTITION_TO_READ_FROM);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        consumer.seek(partitionToReadFrom, TOPIC_PARTITION_OFFSET_TO_READ_FROM);

        //poll the topic for new data
        int noOfMessagesToBeRead = NO_OF_MESSAGES_TO_BE_READ;
        int noOfMessagesReadSoFar = 0;
        boolean  continueIteration= true;

        while(continueIteration){

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info("New message, " +
                        "Topic:" + record.topic() + ", Partition: " +  record.partition() + ", Offset:" + record.offset()  + "\n" +
                        "Key:" + record.key()  + ", Value:" + record.value()
                        );

                noOfMessagesReadSoFar += 1;
                if (noOfMessagesReadSoFar >= noOfMessagesToBeRead) {
                    continueIteration = false; // to exit while loop
                    break; // to exit for look
                }
            }
        }

        logger.info("--------- Exiting the application ---------");
    }

    private static Properties setConfiguration() {
        // Ref: https://kafka.apache.org/20/documentation.html#newconsumerconfigs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

        //earliest: automatically reset the offset to the earliest offset
        //latest: automatically reset the offset to the latest offset
        //none: throw exception to the consumer if no previous offset is found for the consumer's group
        //anything else: throw exception to the consumer.
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest=from-begining??

        return props;
    }
}
