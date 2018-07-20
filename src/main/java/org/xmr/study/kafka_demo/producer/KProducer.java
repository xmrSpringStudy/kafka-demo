package org.xmr.study.kafka_demo.producer;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.security.JaasUtils;
import org.xmr.study.kafka_demo.message.HiMessage;
import org.xmr.study.kafka_demo.message.HiMessageStatus;
import org.xmr.study.kafka_demo.message.HiMessageType;
import org.xmr.study.kafka_demo.util.BytesUtil;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.serializer.DefaultEncoder;
import kafka.serializer.StringEncoder;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

public class KProducer {
    
    public static void main(String[] args) {
    	KProducer producer = new KProducer("test2");
    	producer.run();
    }
    
	private String topic;  
    
    public KProducer(String topic){ 
        this.topic = topic;  
    }  
      
    public void run() { 
    	
    	try
    	{
    		//removeTopic("test2");
        	//createTopic(topic);  
        	//createTopic("test2");      		
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	Producer<String, byte[]> producer = createProducer();  
        int i=0;
        while(true){
        	HiMessage msg = new HiMessage();
        	msg.setType(HiMessageType.Notify);
        	msg.setStatus(HiMessageStatus.Ignore);
        	String body = "message: " + i + "; this is a test";
        	msg.setBody(body.getBytes());
        	byte[] arr = BytesUtil.toByteArray(msg);
            producer.send(new ProducerRecord<String, byte[]>(topic,arr));  
            try {
                TimeUnit.SECONDS.sleep(1);  
            } catch (InterruptedException e) {  
                e.printStackTrace();  
            }  
            
            i++;
            
            if (i > 1000) {
            	break;
            }
        }
    }  
    
    private Producer<String, byte[]> createProducer() { 
    	
        Properties properties = new Properties();  
        properties.put("bootstrap.servers", "localhost:9092");// 声明kafka broker
        properties.put("zookeeper.connect", "127.0.0.1:2181");//声明zk  
        properties.put("serializer.class",  "kafka.serializer.DefaultEncoder");  
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        return new KafkaProducer<String, byte[]>(properties);  
     } 
    
    private ZkUtils createZKUtils() {
    	return ZkUtils.apply("localhost:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
    }
    
    private void removeTopic(String topic) {
    	ZkUtils zkUtils =  createZKUtils();
    	// 删除topic 't1'
    	
    	try {
        	AdminUtils.deleteTopic(zkUtils, topic);    		
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
    	zkUtils.close();
    }
    
    private boolean hasTopic(String topic) {
    	ZkUtils zkUtils =  createZKUtils();
    	
    	// 获取topic 'test'的topic属性属性
    	Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
    	// 查询topic-level属性
    	Iterator it = props.entrySet().iterator();
    	while(it.hasNext()){
    	    Map.Entry entry=(Map.Entry)it.next();
    	    Object key = entry.getKey();
    	    Object value = entry.getValue();
    	    System.out.println(key + " = " + value);
    	}
    	zkUtils.close();
    	
    	return false;
    }
    
    private void createTopic(String topic) {
    	ZkUtils zkUtils =  createZKUtils();
    	
    	if (hasTopic(topic)) {
    			System.out.println("topic " + topic + " is exist");
       		 	zkUtils.close();
    			return;
    	} else {
   		 	System.out.println("topic " + topic + "not exist, we will create it");
   		 	AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), RackAwareMode.Enforced$.MODULE$);
   		 	zkUtils.close();
			return;
    	}    	
    }
}
