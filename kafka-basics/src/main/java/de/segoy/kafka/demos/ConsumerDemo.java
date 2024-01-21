package de.segoy.kafka.demos;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello MotherFuckers, I am a producer");

        String groupId= "my-java-app";
        String topic = "demo_java_2";

        // create Producer Properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");

//        //connect to remote
//        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol","SASL_SSL");
//        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule requiered " +
//                "username=...");
//        properties.setProperty("sasl.mechanism","PLAIN");


       //create Consumer Config
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //latest for just reading new messages
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        //poll for Data
        while(true){
            log.info("polling: ");

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                log.info("Key: "+ record.key() + ", Value: " +record.value());
                log.info("Partition: "+ record.partition() + ", Offset: " +record.offset());
            }
        }


    }
}
