package kaffka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

/**
 * This is extension of ProducerDemoWithCallback program.
 * Here, we want to add keys while sending message
 * Key will ensure that message goes to same partition always
 */
public class ProducerDemoKeys {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
    private static final String KAFKA_TOPIC_NAME = "first_topic";

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
    private static final String TOPIC_FIRST = "first_topic";

    public static void main(String[] args) {
        System.out.println("Hello World");

        //*** 3 steps to produce message by producer ***

        //1. create producer properties
        Properties props = setProperties();

        //----------------------------

        //2.a create producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);

        String msg_value="";
        String msg_key="";

        for(int i=1; i<=10; i++) {
            msg_value="content for message-" + i;
            msg_key="id_" + i;

            //2.b Create a producer record
            //ProducerRecord<String, String> record = new ProducerRecord(KAFKA_TOPIC_NAME, msg_key, msg_value);
            ProducerRecord<String, String> record
                    = new ProducerRecord(
                    TOPIC_FIRST,
                    UUID.randomUUID() + " @ "+ LocalDateTime.now());

            //----------------------------

            //3.a send data - Async

            //producer.send(record);
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //This block is executed everytime record is successfully sent or an exception is thrown
                    if(e == null) {
                        //record sent successfully.
                        logger.info("Received metadata. \n" +
                                "Topic: " + recordMetadata.topic() + " \n " +
                                "Partition: " + recordMetadata.partition() + " \n " +
                                "Offset: " + recordMetadata.offset() + " \n " +
                                "Timestamp: " + recordMetadata.timestamp()
                        );
                    } else {
                        logger.error("Error occured while producing :", e);
                    }
                }
            });
        }

        //3.b flush data
        producer.flush();
        //3.c close producer
        producer.close();
    }

    private static Properties setProperties() {
        Properties props = new Properties();

        //A list of host/port pairs to use for establishing the initial connection to the Kafka cluster.
        //The client will make use of all servers irrespective of which servers are specified here
        // for bootstrapping—this list only impacts the initial hosts used to discover the full set of
        // servers. This list should be in the form host1:port1,host2:port2,....
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // Kafka client will send everything into bytes (0 and 1),
        // here we are sending String so used StringSerializer
        //Serializer class for key that implements the org.apache.kafka.common.serialization.Serializer interface.
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Serializer class for value that implements the org.apache.kafka.common.serialization.Serializer interface.
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }
}

