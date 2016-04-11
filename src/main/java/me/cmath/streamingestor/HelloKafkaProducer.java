package me.cmath.streamingestor;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import scala.collection.Seq;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class HelloKafkaProducer {
    final static String TOPIC = "pythontest";

    public HelloKafkaProducer() {
    	
    }
    
    public void getMessage(String m) {
    	Properties properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        ProducerConfig producerConfig = new ProducerConfig(properties);
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);        
    	KeyedMessage<String, String> message = new KeyedMessage<String, String>(TOPIC, m);
        producer.send(message);
        producer.close();

    }
  
}
