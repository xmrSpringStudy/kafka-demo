package org.xmr.study.kafka_demo.consumer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.xmr.study.kafka_demo.message.HiMessage;
import org.xmr.study.kafka_demo.producer.KProducer;
import org.xmr.study.kafka_demo.util.BytesUtil;

import kafka.serializer.DefaultEncoder;

public class KConsumer {
    
    public static void main(String[] args) {
    	KConsumer producer = new KConsumer("test");
    	producer.run();
    }
    
	private String topic;  
	
	public KConsumer(String topic) {
		this.topic = topic;
	}

    public void run() {  
    	 System.out.println("start consumer");
    	Properties props = new Properties();
        props.put("bootstrap.servers", "127.0.0.1:9092");
        //props.put("zookeeper.connect", "127.0.0.1:2181");//声明zk  
        props.put("serializer.class", DefaultEncoder.class.getName());  
        props.put("group.id", "hello-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		
        Collection<String> topics = new ArrayList<String>();
        topics.add("test");
        topics.add("test2");
        consumer.subscribe(topics, new ConsumerRebalanceListener() {

			public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
				// TODO Auto-generated method stub
				
			}

			public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
				// TODO Auto-generated method stub
				
			}
        });
		

   	 System.out.println("create consumer");
        while (true) {
            ConsumerRecords<String, byte[]> records = consumer.poll(100);
            for (ConsumerRecord<String, byte[]> record : records) {
            	byte[] bytes = record.value();
            	HiMessage msg = (HiMessage)BytesUtil.toObject(bytes);
            	byte[] body = msg.getBody();
            	String content = "";
            	if (body != null) {
            		content = new String(body);
            	}
                System.out.printf("offset = %d, key = %s, topic:%s, value = %s", record.offset(), record.key(), record.topic(), content);
                System.out.println();
            }
 
            try {
                TimeUnit.MICROSECONDS.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }		
    }  

}
