package kaffka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String CONSUMER_GROUP_ID = "my-first-application";
    //TODO Set the topic
    private static final String TOPIC_SUBSCRIBED = "first_topic";

    public static void main(String[] args) {

        //1. create consumer config
        Properties props = setConfiguration();

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(props);

        //subscribe consumer to topic(s)
        //consumer.subscribe(Collections.singletonList(TOPIC_SUBSCRIBED));
        consumer.subscribe(Arrays.asList(TOPIC_SUBSCRIBED));

        //poll the topic for new data
        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, String> record : records){
                logger.info("New message, " +
                        "Topic:" + record.topic() + ", Partition: " +  record.partition() + ", Offset:" + record.offset()  + "\n" +
                        "Key:" + record.key()  + ", Value:" + record.value()
                        );
            }
        }
    }

    private static Properties setConfiguration() {
        // Ref: https://kafka.apache.org/20/documentation.html#newconsumerconfigs
        Properties props = new Properties();

        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP_ID);

        //earliest: automatically reset the offset to the earliest offset
        //latest: automatically reset the offset to the latest offset
        //none: throw exception to the consumer if no previous offset is found for the consumer's group
        //anything else: throw exception to the consumer.
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // earliest=from-begining??

        return props;
    }
}
