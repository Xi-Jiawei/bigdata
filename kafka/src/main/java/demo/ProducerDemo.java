package demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
    public static void main(String[] args){
        //String server_ip="172.31.42.237:9092";
        //String server_ip="192.168.61.130:9092";
        String server_ip="47.98.47.81:9092";
        Properties properties = new Properties();
        //properties.put("bootstrap.servers", "172.31.42.237:9092");//xxx服务器ip
        //properties.put("bootstrap.servers", "192.168.61.130:9092");//xxx服务器ip
        properties.put("bootstrap.servers", "47.98.47.81:9092");//xxx服务器ip
        properties.put("acks", "all");//所有follower都响应了才认为消息提交成功，即"committed"
        properties.put("retries", 0);//retries = MAX 无限重试，直到你意识到出现了问题:)
        properties.put("batch.size", 16384);//producer将试图批处理消息记录，以减少请求次数.默认的批量处理消息字节数
        //batch.size当批量的数据大小达到设定值后，就会立即发送，不顾下面的linger.ms
        properties.put("linger.ms", 1);//延迟1ms发送，这项设置将通过增加小的延迟来完成--即，不是立即发送一条记录，producer将会等待给定的延迟时间以允许其他消息记录发送，这些消息记录可以批量处理
        properties.put("buffer.memory", 33554432);//producer可以用来缓存数据的内存大小。
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = null;
        try {
            producer = new KafkaProducer<String, String>(properties);
            for (int i = 0; i < 5; i++) {
                String msg = "from "+server_ip+": " + "message " + i;
                producer.send(new ProducerRecord<String, String>("mykafka", msg));
                System.out.println(msg);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
