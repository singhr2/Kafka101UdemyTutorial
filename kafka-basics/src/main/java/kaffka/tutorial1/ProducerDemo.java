package kaffka.tutorial1;

/**
 * Before Running this, ensure that Kafka , Zookeeper and Some Consumer is running,
 * so that you can see the message consumed in Consumer console
 *
 * [Start Zookeeper] - Windows OS
 * $ zookeeper-server-start.bat C:\SW\kafka_2.13-2.5.0\config\zookeeper.properties
 *
 * [Start Kafka] - Windows OS
 * $ kafka-server-start.bat C:\SW\kafka_2.13-2.5.0\config\server.properties
 *
 * [Start Kafka Consumer] - Windows OS
 * $ kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic first_topic --group my-third-appln
 *
 */

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

public class ProducerDemo {

    private static final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";

    //TODO Set the topic
    public static final String TOPIC_FIRST = "first_topic";
    //public static final String MESSAGE_VALUE = "Hello Kafka !";

    public static void main(String[] args) {
        System.out.println("Hello World");

        //*** 3 steps to produce message by producer ***

        //1. create producer config
        Properties props = setConfiguration();

        //----------------------------

        //2.a create producer
        KafkaProducer<String, String> producer = new KafkaProducer(props);

        //2.b Create a producer record
        ProducerRecord<String, String> record
                = new ProducerRecord(
                        TOPIC_FIRST,
                UUID.randomUUID() + " @ "+ LocalDateTime.now()); // value of interest

        //----------------------------

        //3.a send data - Async
        producer.send(record);
        //3.b flush data
        producer.flush();
        //3.c close producer
        producer.close();
    }

    private static Properties setConfiguration() {
        Properties props = new Properties();

        //Ref: https://kafka.apache.org/20/documentation.html#producerconfigs

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
