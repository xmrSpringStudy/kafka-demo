package org.xmr.study.kafka_demo.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import kafka.serializer.StringEncoder;

public class KProducer {
    
    public static void main(String[] args) {
    	KProducer producer = new KProducer("test");
    	producer.run();
    }
    
	private String topic;  
    
    public KProducer(String topic){ 
        this.topic = topic;  
    }  
      
    public void run() { 
    	Producer<String, String> producer = createProducer();  
        int i=0;
        while(true){
            producer.send(new ProducerRecord<String, String>(topic, "message: " + i++));  
            try {
                TimeUnit.SECONDS.sleep(1);  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
            
            if (i > 1000) {
            	break;
            }
        }
    }  
    
    private Producer<String, String> createProducer() { 
    	
        Properties properties = new Properties();  
        properties.put("bootstrap.servers", "localhost:9092");// 声明kafka broker
        properties.put("zookeeper.connect", "127.0.0.1:2181");//声明zk  
        properties.put("serializer.class", StringEncoder.class.getName());  
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<String, String>(properties);  
     } 
}
