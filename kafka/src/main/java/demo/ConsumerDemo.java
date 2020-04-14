package demo;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerDemo {
    public static void main(String[] args){
        String topic="mykafka";
        AdminClient adminClient;
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "172.31.42.237:9092,172.31.43.12:9092,172.31.43.21:9092");
        //properties.put("bootstrap.servers", "192.168.61.130:9092,192.168.61.131:9092,192.168.61.132:9092");
        properties.put("bootstrap.servers", "47.98.176.164:9092,47.98.47.81:9092,116.62.119.79:9092");
        adminClient = AdminClient.create(properties);
        //删除topic
        try {
            List<String> topics=new ArrayList<String>();
            topics.add(topic);
            adminClient.deleteTopics(topics);
            adminClient.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        //创建topic
        try {
            NewTopic newTopic = new NewTopic(topic,1,(short) 3);//partitions:1; replication-factor:3
            adminClient.createTopics(Arrays.asList(newTopic));
            adminClient.close();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
        }

        //连接Kafka，启动消费者
        //Properties properties = new Properties();
        properties = new Properties();
        //properties.put("bootstrap.servers", "172.31.43.12:9092");//xxx是服务器集群的ip
        //properties.put("bootstrap.servers", "192.168.61.131:9092");//xxx是服务器集群的ip
        properties.put("bootstrap.servers", "47.98.176.164:9092");//xxx是服务器集群的ip
        properties.put("group.id", "test-consumer-group");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "latest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));
        Instant begin = Instant.now();
        Instant end = Instant.now();
        Duration duration=Duration.between(begin,end);
        while (true) {
            //ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            ConsumerRecords<String, String> records = kafkaConsumer.poll(duration);
            for (ConsumerRecord<String, String> record : records) {
                //System.out.println("-----------------");
                //System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println("consumer receive: "+record.value());
            }
        }
    }
}
