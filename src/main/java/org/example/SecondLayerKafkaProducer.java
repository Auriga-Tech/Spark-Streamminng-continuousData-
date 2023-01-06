package org.example;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class SecondLayerKafkaProducer {
    private static String KafkaBrokerEndpoint = "{IP:HOST}";
    private static String KafkaTopic = "{TOPIC_NAME_WHERE_FILTERED_DATA RECEIVED}";

    private Producer<String, Object> ProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "NDMS");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<String, Object>(properties);
    }

    public void DataTransfer(List<Object> allRecord) {

        SecondLayerKafkaProducer secondLayerKafkaProducer = new SecondLayerKafkaProducer();
        secondLayerKafkaProducer.PublishMessages(allRecord);
    }

    //  Method used to publish a message on a kafka topic
    private void PublishMessages(List<Object> allRecord) {

        final Producer<String, Object> NdmsProducer = ProducerProperties();

        allRecord.forEach(line -> {
            System.out.println(line);

            final ProducerRecord<String, Object> wikiRecord = new ProducerRecord<String, Object>(
                    KafkaTopic, UUID.randomUUID().toString(), line.toString());

//  Sending data on kafka topic (second layer)
            NdmsProducer.send(wikiRecord, (metadata, exception) -> {

            });
        });

    }
}
