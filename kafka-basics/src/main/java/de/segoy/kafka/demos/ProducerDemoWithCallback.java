package de.segoy.kafka.demos;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello MotherFuckers, I am a producer");

        // create Producer Properties
        Properties properties = new Properties();

        //connect to localhost
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");

//        //connect to remote
//        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
//        properties.setProperty("security.protocol","SASL_SSL");
//        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule
//        requiered " +
//                "username=...");
//        properties.setProperty("sasl.mechanism","PLAIN");

        // Set Producer Properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "400");
//        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int j = 0; j<10; j++){
        for (int i = 0; i < 30; i++) {


            //create Producer Record
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("demo_java_2", "hello World");

            //Send Data
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    //executes always after send
                    if (exception == null) {
                        log.info("Received new metadata \n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp() + "\n"
                        );
                    } else {
                        log.error(exception.getMessage());
                    }
                }
            });
        }
        try {
            Thread.sleep(500L);
        }catch(Exception e){

        }}
        //tell the producer to send all data and block until done -- synchronous
        producer.flush();

        //flush and close the producer *calls flush too*
        producer.close();

    }
}
