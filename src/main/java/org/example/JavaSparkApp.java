package org.example;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import org.apache.kafka.common.serialization.StringDeserializer;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class JavaSparkApp {
    public static void main(String[] args) throws InterruptedException {
     
        Logger.getLogger("org").setLevel(Level.ALL);
        Logger.getLogger("akka").setLevel(Level.ALL);

   // Firstly, we'll begin by initializing the JavaStreamingContext which is the entry point for all Spark Streaming applications:

        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.setAppName("SparkStreamming");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(20));

        // Now, we can connect to the Kafka topic from the JavaStreamingContext.

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "server_IP : Port"); 
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "xyz");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("Ndms"); //topic name if multiple then all seperated by comma



        //List<Object>allRecord
        JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));

        List<Object> allRecord=new ArrayList<>();
        messages.foreachRDD(rdd->{
            System.out.println("the count is"+rdd.count());
            rdd.collect().forEach(raw->{
                System.out.println("The Data is "+raw);
            });

            JavaRDD<ConsumerRecord<String, String>> filtered_rdd =rdd.filter(new Function<ConsumerRecord<String,String>>(){
                public Boolean call(String str) {

                    return str.contains("\"status\": 1");

                }

            });
            if(filtered_rdd.count() > 0) {

                filtered_rdd.collect().forEach(rawRecord -> {
//                  String record = rawRecord._2();
                    allRecord.add(rawRecord);
                });

//  method calling to send data to second layer of kafka
                if(allRecord.size()>0){
                    SecondLayerKafkaProducer obj=new SecondLayerKafkaProducer();
                    obj.DataTransfer(allRecord);
                    System.out.println("Data is Transfer to kafka layer 2 ");
                }


                System.out.println("All records added:"+allRecord.size() + " rdd count:" +rdd.count());
            }

        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
