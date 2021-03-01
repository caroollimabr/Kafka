package br.com.carol.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys { //keys allows us to ensure that the messages that have the same keys go to the same partition
//if we don't use a key, the messages will be sent to any partition (having in mind the ones we have)

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create Producer properties: https://kafka.apache.org/documentation/#producerconfigs
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //value will be a StringSerializer, give the class and the name
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); //the key and the value are strings

        for (int i = 0; i<10; i++) { //sends 10 messages/records

            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            //create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            logger.info("Key: " + key); // log the key --we guarantee that the same key goes to the same partition
            //for 3 partitions:
            // id_0 goes to partition 1
            // id_1 goes to partition 0
            // id_2 goes to partition 2
            // id_3 goes to partition 0
            // id_4 goes to partition 2
            // id_5 goes to partition 2
            // id_6 goes to partition 0
            // id_7 goes to partition 2
            // id_8 goes to partition 1
            // id_9 goes to partition 2


            //send data to the topic - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        //the record is successfully sent
                        logger.info("received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // block the .send() in order to make it synchronous - don't do it in production!

        }

        //flush data and close producer
        producer.flush();
        producer.close();

    }

}
